"""
Incremental extract: GitHub REST API -> S3 raw.

Second, deliberately simpler ingestion source (per project doc) -- this
script exists mainly to demonstrate the API ingestion pattern itself
(pagination + multithreading), not deep business value from the data.

Pulls 4 entities from a single public repo:
    - repositories   : one object, the repo's own metadata
    - issues         : supports incremental extraction via GitHub's
                        `since` query param -- the API itself filters
                        server-side, so we just pass our stored watermark
    - pull_requests   : GitHub's /pulls endpoint has NO `since` param, so
                        instead we sort by `updated desc` and stop paging
                        as soon as we see an item older than our watermark
                        (a different, equally valid incremental pattern --
                        good talking point: not every API supports the
                        same incremental mechanism)
    - contributors    : GitHub returns this as pre-aggregated stats with
                        no updated_at at all -- there is nothing to filter
                        incrementally on, so this is a full re-pull every run

Pagination: GitHub returns a `Link` response header with rel="next"/"last"
pointing to further pages -- we follow it rather than guessing page counts.

Multithreading: for issues and contributors, page 1 is fetched first to
discover the total page count (from the `Link: rel="last"` URL), then
pages 2..N are fetched CONCURRENTLY via ThreadPoolExecutor. pull_requests
is fetched sequentially instead, because its early-stop logic depends on
processing pages in order (see note above).

Usage:
    uv run python scripts/github_extract.py
"""

import json
import os
import re
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import requests
from dotenv import load_dotenv

load_dotenv()

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
REPO = "apache/airflow"  # owner/repo -- swap for any public repo
API_BASE = "https://api.github.com"
PER_PAGE = 100
MAX_WORKERS = 5  # concurrent page fetches -- polite to GitHub's rate limiter

S3_BUCKET_NAME = "tp-portfolio-ecommerce-raw"
STATE_FILE = Path(__file__).parent / "state" / "github_watermark_state.json"

EPOCH_START = datetime(1970, 1, 1, tzinfo=timezone.utc)


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


MAX_PAGES_PULLS = 20  # hard safety cap -- 20 pages * 100/page = 2000 most-recent PRs max


def fetch_pulls_since(url: str, since: datetime) -> list[dict]:
    """No `since` param exists for /pulls, so we sort by updated desc and
    walk pages SEQUENTIALLY, stopping as soon as a page's items are all
    older than our watermark -- no point fetching further, everything
    after this point is data we've already seen. Also capped at
    MAX_PAGES_PULLS as a hard safety net against pathologically large
    histories."""
    params = {"state": "all", "sort": "updated", "direction": "desc", "per_page": PER_PAGE}
    collected = []
    page = 1
    while page <= MAX_PAGES_PULLS:
        print(f"    ...fetching pull_requests page {page}", flush=True)
        response = requests.get(url, headers=github_headers(), params={**params, "page": page}, timeout=30)
        response.raise_for_status()
        items = response.json()
        if not items:
            break

        new_items = [item for item in items if parse_gh_datetime(item["updated_at"]) > since]
        collected.extend(new_items)
        print(f"    ...page {page}: {len(new_items)}/{len(items)} rows newer than watermark", flush=True)

        if len(new_items) < len(items):
            # hit at least one item at/before the watermark -> everything
            # after this page (sorted descending) is old news, stop here
            break
        page += 1
    else:
        print(f"    ...hit MAX_PAGES_PULLS ({MAX_PAGES_PULLS}) safety cap, stopping early", flush=True)

    return collected


def parse_gh_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def rows_to_jsonl(rows: list[dict]) -> str:
    return "\n".join(json.dumps(row) for row in rows)


def upload_to_s3(s3_client, entity_name: str, run_id: str, jsonl_content: str) -> str:
    ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"raw/github_{entity_name}/ingestion_date={ingestion_date}/run_id={run_id}/{entity_name}.jsonl"
    s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=key, Body=jsonl_content.encode("utf-8"))
    return key


def load_watermark_state() -> dict:
    if STATE_FILE.exists():
        with open(STATE_FILE) as f:
            return json.load(f)
    return {}


def save_watermark_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def extract_repository(s3_client, run_id: str, watermark_state: dict):
    response = requests.get(f"{API_BASE}/repos/{REPO}", headers=github_headers(), timeout=30)
    response.raise_for_status()
    repo_data = response.json()

    last_updated_str = watermark_state.get("repositories")
    last_updated = parse_gh_datetime(last_updated_str) if last_updated_str else EPOCH_START
    current_updated = parse_gh_datetime(repo_data["updated_at"])

    if current_updated <= last_updated:
        print(f"  repositories: no change since {last_updated.isoformat()}")
        return

    s3_key = upload_to_s3(s3_client, "repositories", run_id, rows_to_jsonl([repo_data]))
    watermark_state["repositories"] = repo_data["updated_at"]
    print(f"  repositories: extracted 1 row -> s3://{S3_BUCKET_NAME}/{s3_key}")


def extract_issues(s3_client, run_id: str, watermark_state: dict):
    last_updated_str = watermark_state.get("issues")
    since = parse_gh_datetime(last_updated_str) if last_updated_str else EPOCH_START

    # NOTE: GitHub's API silently returns an empty result set when `since`
    # is set to the epoch (1970-01-01) -- confirmed by testing (any real
    # date like 2020+ works fine, only the epoch value breaks). Likely an
    # internal date-parsing edge case on GitHub's side, not documented.
    # Fix: on the very first run (no stored watermark yet), omit `since`
    # entirely -- `state=all` alone already returns the complete history,
    # which is exactly what we want for a first pull anyway.
    params = {"state": "all", "per_page": PER_PAGE}
    if last_updated_str:
        params["since"] = since.isoformat().replace("+00:00", "Z")

    # GitHub's /issues endpoint also returns pull requests mixed in,
    # distinguished only by the presence of a "pull_request" key -- filter
    # those out, they're handled separately by extract_pull_requests.
    raw_items = fetch_paginated_concurrent(f"{API_BASE}/repos/{REPO}/issues", params)
    issues_only = [item for item in raw_items if "pull_request" not in item]

    if not issues_only:
        print(f"  issues: no new rows since {since.isoformat()}")
        return

    s3_key = upload_to_s3(s3_client, "issues", run_id, rows_to_jsonl(issues_only))
    newest = max(parse_gh_datetime(item["updated_at"]) for item in issues_only)
    watermark_state["issues"] = newest.isoformat()
    print(f"  issues: extracted {len(issues_only)} rows -> s3://{S3_BUCKET_NAME}/{s3_key}")


# On a very first run there's no watermark yet, and walking ALL of a large
# repo's PR history sequentially (required for the early-stop logic below)
# can mean hundreds of pages / many minutes. Bound the first run's lookback
# to a reasonable window instead -- same 90-day window used for the
# Postgres seed data, kept consistent across the project. Subsequent runs
# use the real watermark and only ever look at what's genuinely new.
INITIAL_LOOKBACK_DAYS = 90


def extract_pull_requests(s3_client, run_id: str, watermark_state: dict):
    last_updated_str = watermark_state.get("pull_requests")
    if last_updated_str:
        since = parse_gh_datetime(last_updated_str)
    else:
        since = datetime.now(timezone.utc) - timedelta(days=INITIAL_LOOKBACK_DAYS)

    new_pulls = fetch_pulls_since(f"{API_BASE}/repos/{REPO}/pulls", since)

    if not new_pulls:
        print(f"  pull_requests: no new rows since {since.isoformat()}")
        return

    s3_key = upload_to_s3(s3_client, "pull_requests", run_id, rows_to_jsonl(new_pulls))
    newest = max(parse_gh_datetime(item["updated_at"]) for item in new_pulls)
    watermark_state["pull_requests"] = newest.isoformat()
    print(f"  pull_requests: extracted {len(new_pulls)} rows -> s3://{S3_BUCKET_NAME}/{s3_key}")


def extract_contributors(s3_client, run_id: str):
    # No updated_at on this endpoint at all -- always a full re-pull.
    params = {"per_page": PER_PAGE}
    contributors = fetch_paginated_concurrent(f"{API_BASE}/repos/{REPO}/contributors", params)

    s3_key = upload_to_s3(s3_client, "contributors", run_id, rows_to_jsonl(contributors))
    print(f"  contributors: extracted {len(contributors)} rows (full re-pull) -> s3://{S3_BUCKET_NAME}/{s3_key}")


def main():
    if not GITHUB_TOKEN:
        print("WARNING: no GITHUB_TOKEN found in .env -- proceeding unauthenticated (60 req/hour limit).")

    run_id = uuid.uuid4().hex[:12]
    print(f"Starting GitHub extract for {REPO}, run_id={run_id}")

    watermark_state = load_watermark_state()
    s3_client = boto3.client("s3")

    extract_repository(s3_client, run_id, watermark_state)
    extract_issues(s3_client, run_id, watermark_state)
    extract_pull_requests(s3_client, run_id, watermark_state)
    extract_contributors(s3_client, run_id)

    save_watermark_state(watermark_state)
    print("Done. Watermark state saved.")


if __name__ == "__main__":
    main()
