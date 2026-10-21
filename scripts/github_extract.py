"""
Windowed extract: GitHub REST API -> S3 raw (append-only).

Second, deliberately simpler ingestion source (per project doc) -- this
script exists mainly to demonstrate the API ingestion pattern itself
(pagination + multithreading), not deep business value from the data.

Pulls 4 entities from a single public repo for the half-open window
[--start, --end) passed by the caller (ADR-006: no watermark state; in
Airflow the window is the run's data interval minus a lookback). The
GitHub API has no uniform "from-to" filter, so each entity gets its own rule:

    - repositories  : one object, the repo's own metadata. Full snapshot on
                      every run, no window (it is a single current-state row).
    - issues        : `since=start` is filtered server-side (updated_at >=
                      since); the upper bound `updated_at < end` is applied
                      client-side, because the API has no `until` param.
    - pull_requests : /pulls has NO `since` param at all, so we sort by
                      `updated desc`, skip items newer than `end`, keep the
                      window, and stop paging at the first item older than
                      `start` (a different, equally valid incremental pattern
                      -- good talking point: not every API supports the same
                      incremental mechanism).
    - contributors  : pre-aggregated stats with no updated_at at all --
                      nothing to filter on, full snapshot every run.

Backfill caveat (ADR-006): the API returns the CURRENT state of each object.
An old window only gets items whose *current* updated_at falls inside it,
and repositories/contributors are always "as of today". Re-running a window
never duplicates anything downstream (MERGE dedups by key), but it does not
reproduce what the API looked like back then.

Pagination: GitHub returns a `Link` response header with rel="next"/"last"
pointing to further pages -- we follow it rather than guessing page counts.

Multithreading: for issues and contributors, page 1 is fetched first to
discover the total page count (from the `Link: rel="last"` URL), then
pages 2..N are fetched CONCURRENTLY via ThreadPoolExecutor. pull_requests
is fetched sequentially instead, because its early-stop logic depends on
processing pages in order (see note above).

Usage:
    uv run python scripts/github_extract.py --start 2026-10-20T00:00:00+00:00 --end 2026-10-21T00:00:00+00:00
Timestamps without a timezone are treated as UTC.
"""

import argparse
import json
import os
import re
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import boto3
import requests
from dotenv import load_dotenv

load_dotenv()  # local runs only; inside Docker the variables come from docker-compose

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
REPO = os.environ.get("GITHUB_REPO", "apache/airflow")  # owner/repo -- swap for any public repo
API_BASE = "https://api.github.com"
PER_PAGE = 100
MAX_WORKERS = 5  # concurrent page fetches -- polite to GitHub's rate limiter

S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "tp-portfolio-ecommerce-raw")

# GitHub silently returns an EMPTY result when `since` is the Unix epoch
# (found by testing in Phase 1, not documented). Any window starting before
# GitHub existed means "everything", so `since` is simply omitted then.
GITHUB_FOUNDED = datetime(2008, 1, 1, tzinfo=timezone.utc)

MAX_PAGES_PULLS = 20  # hard safety cap -- 20 pages * 100/page = 2000 most-recently-updated PRs max


def parse_utc_timestamp(value: str) -> datetime:
    # Airflow passes ISO strings with an offset; a bare date typed by hand is read as UTC.
    parsed = datetime.fromisoformat(value)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract one time window from the GitHub API to S3 raw.")
    parser.add_argument("--start", required=True, type=parse_utc_timestamp, help="window start, inclusive (ISO 8601)")
    parser.add_argument("--end", required=True, type=parse_utc_timestamp, help="window end, exclusive (ISO 8601)")
    args = parser.parse_args()
    if args.start >= args.end:
        parser.error(f"--start ({args.start.isoformat()}) must be earlier than --end ({args.end.isoformat()})")
    return args


def github_headers() -> dict:
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    return headers


def parse_last_page(link_header: str | None) -> int:
    """Extract the last page number from a GitHub `Link` header, e.g.:
    '<...&page=2>; rel="next", <...&page=9>; rel="last"' -> 9
    Returns 1 if there's no `Link` header at all (single-page result)."""
    if not link_header:
        return 1
    match = re.search(r'[?&]page=(\d+)>; rel="last"', link_header)
    return int(match.group(1)) if match else 1


def parse_gh_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def in_window(item: dict, window_start: datetime, window_end: datetime) -> bool:
    return window_start <= parse_gh_datetime(item["updated_at"]) < window_end


def fetch_page(url: str, params: dict, page: int) -> tuple[int, list[dict]]:
    response = requests.get(url, headers=github_headers(), params={**params, "page": page}, timeout=30)
    response.raise_for_status()
    return page, response.json()


def fetch_paginated_concurrent(url: str, params: dict) -> list[dict]:
    """Fetch page 1 to learn the total page count, then fetch the rest
    concurrently. Order of results doesn't matter here (we're landing raw
    data, not depending on sort order downstream)."""
    first_page_response = requests.get(url, headers=github_headers(), params={**params, "page": 1}, timeout=30)
    first_page_response.raise_for_status()
    all_items = first_page_response.json()

    last_page = parse_last_page(first_page_response.headers.get("Link"))
    if last_page <= 1:
        return all_items

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_page, url, params, page) for page in range(2, last_page + 1)]
        for future in as_completed(futures):
            _, items = future.result()
            all_items.extend(items)

    return all_items


def fetch_pulls_in_window(url: str, window_start: datetime, window_end: datetime) -> list[dict]:
    """No `since` param exists for /pulls, so we sort by updated desc and
    walk pages SEQUENTIALLY: items newer than the window are skipped, items
    inside it are kept, and paging stops at the first item older than the
    window -- everything after it (sorted descending) is older still.
    Capped at MAX_PAGES_PULLS as a hard safety net; hitting the cap before
    reaching an older item means the window was NOT fully covered (typical
    for a backfill of a window far in the past on a very active repo)."""
    params = {"state": "all", "sort": "updated", "direction": "desc", "per_page": PER_PAGE}
    collected = []
    page = 1
    reached_older_than_window = False
    while page <= MAX_PAGES_PULLS:
        print(f"    ...fetching pull_requests page {page}", flush=True)
        response = requests.get(url, headers=github_headers(), params={**params, "page": page}, timeout=30)
        response.raise_for_status()
        items = response.json()
        if not items:
            reached_older_than_window = True  # ran out of PRs entirely
            break

        window_items = [item for item in items if in_window(item, window_start, window_end)]
        collected.extend(window_items)
        print(f"    ...page {page}: {len(window_items)}/{len(items)} rows in window", flush=True)

        if parse_gh_datetime(items[-1]["updated_at"]) < window_start:
            reached_older_than_window = True
            break
        page += 1

    if not reached_older_than_window:
        print(
            f"    WARNING: hit MAX_PAGES_PULLS ({MAX_PAGES_PULLS}) before reaching {window_start.isoformat()} "
            "-- window not fully covered, older PRs in it were skipped",
            flush=True,
        )
    return collected


def rows_to_jsonl(rows: list[dict]) -> str:
    return "\n".join(json.dumps(row) for row in rows)


def upload_to_s3(s3_client, entity_name: str, run_id: str, jsonl_content: str) -> str:
    ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"raw/github_{entity_name}/ingestion_date={ingestion_date}/run_id={run_id}/{entity_name}.jsonl"
    s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=key, Body=jsonl_content.encode("utf-8"))
    return key


def land_rows(s3_client, entity_name: str, run_id: str, rows: list[dict], note: str = "") -> int:
    if not rows:
        print(f"  {entity_name}: no rows{note}")
        return 0
    s3_key = upload_to_s3(s3_client, entity_name, run_id, rows_to_jsonl(rows))
    print(f"  {entity_name}: extracted {len(rows)} rows{note} -> s3://{S3_BUCKET_NAME}/{s3_key}")
    return len(rows)


def extract_repository(s3_client, run_id: str) -> int:
    # Single current-state object -- full snapshot every run, no window.
    response = requests.get(f"{API_BASE}/repos/{REPO}", headers=github_headers(), timeout=30)
    response.raise_for_status()
    return land_rows(s3_client, "repositories", run_id, [response.json()], " (full snapshot)")


def extract_issues(s3_client, run_id: str, window_start: datetime, window_end: datetime) -> int:
    params = {"state": "all", "per_page": PER_PAGE}
    if window_start >= GITHUB_FOUNDED:
        params["since"] = window_start.isoformat().replace("+00:00", "Z")

    # /issues also returns pull requests mixed in, distinguished only by the
    # presence of a "pull_request" key -- those are handled by extract_pull_requests.
    # `since` bounds only the start, so the window end is applied here.
    raw_items = fetch_paginated_concurrent(f"{API_BASE}/repos/{REPO}/issues", params)
    window_issues = [
        item for item in raw_items
        if "pull_request" not in item and in_window(item, window_start, window_end)
    ]
    return land_rows(s3_client, "issues", run_id, window_issues)


def extract_pull_requests(s3_client, run_id: str, window_start: datetime, window_end: datetime) -> int:
    window_pulls = fetch_pulls_in_window(f"{API_BASE}/repos/{REPO}/pulls", window_start, window_end)
    return land_rows(s3_client, "pull_requests", run_id, window_pulls)


def extract_contributors(s3_client, run_id: str) -> int:
    # No updated_at on this endpoint at all -- full snapshot every run.
    contributors = fetch_paginated_concurrent(f"{API_BASE}/repos/{REPO}/contributors", {"per_page": PER_PAGE})
    return land_rows(s3_client, "contributors", run_id, contributors, " (full snapshot)")


def main():
    args = parse_args()
    if not GITHUB_TOKEN:
        print("WARNING: no GITHUB_TOKEN set -- proceeding unauthenticated (60 req/hour limit).")

    run_id = uuid.uuid4().hex[:12]
    print(f"Starting GitHub extract for {REPO}, run_id={run_id}, window=[{args.start.isoformat()}, {args.end.isoformat()})")

    s3_client = boto3.client("s3")
    total_rows = (
        extract_repository(s3_client, run_id)
        + extract_issues(s3_client, run_id, args.start, args.end)
        + extract_pull_requests(s3_client, run_id, args.start, args.end)
        + extract_contributors(s3_client, run_id)
    )
    print(f"Done. {total_rows} rows extracted across 4 entities.")


if __name__ == "__main__":
    main()
