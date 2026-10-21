"""
Incremental extract: postgres-source -> S3 raw (append-only).

For each table, pulls the rows whose watermark column falls inside the
half-open window [--start, --end), writes them as JSON Lines, and uploads
to S3 under a path partitioned by ingestion date + run_id (never
overwriting anything -- see ADR-002).

No watermark state is stored anywhere (ADR-006). The caller decides the
window: in Airflow it is the run's data interval, with a lookback already
subtracted from the start by the task (the script never adds an overlap
itself). Rows picked up twice because of that overlap -- or because a run
is retried/backfilled -- are expected: merge_*.py deduplicates by primary
key before MERGE, so the same window always ends in the same Silver state.

Why the overlap exists at all: in Postgres now() (and DEFAULT now() on
updated_at) is the transaction START time, not commit time. A transaction
that starts before a run reads the table and commits after it gets an
updated_at inside the already-processed window; without a lookback that
row would never be extracted.

The script knows nothing about Airflow. Connection settings come from
environment variables, defaulting to the local docker-compose values, so
the same file runs on the laptop (localhost) and inside the Airflow
container (POSTGRES_SOURCE_HOST=postgres-source).

Usage:
    uv run python scripts/extract.py --start 2026-10-05T00:00:00+00:00 --end 2026-10-06T00:00:00+00:00
    # first full load:
    uv run python scripts/extract.py --start 1970-01-01 --end 2026-10-07
Timestamps without a timezone are treated as UTC.
"""

import argparse
import json
import os
import uuid
from datetime import datetime, timezone

import boto3
import psycopg2
import psycopg2.extras

# --- Connection settings: env vars, defaults match docker-compose.yml ---
DB_CONFIG = {
    "host": os.environ.get("POSTGRES_SOURCE_HOST", "localhost"),
    "port": int(os.environ.get("POSTGRES_SOURCE_PORT", "5432")),
    "dbname": os.environ.get("POSTGRES_SOURCE_DB", "ecommerce"),
    "user": os.environ.get("POSTGRES_SOURCE_USER", "source_user"),
    "password": os.environ.get("POSTGRES_SOURCE_PASSWORD", "source_password"),
}

S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "tp-portfolio-ecommerce-raw")

# table_name -> watermark column. order_status_history uses changed_at
# (append-only, no updated_at -- see ddl.sql / cheat sheet 19.5).
TABLES = {
    "customers": "updated_at",
    "products": "updated_at",
    "orders": "updated_at",
    "order_items": "updated_at",
    "payments": "updated_at",
    "order_status_history": "changed_at",
}


def parse_utc_timestamp(value: str) -> datetime:
    # Airflow passes ISO strings with an offset (2026-10-05T00:00:00+00:00);
    # a bare date or naive timestamp typed by hand is read as UTC, so the
    # comparison against TIMESTAMPTZ columns is never session-timezone dependent.
    parsed = datetime.fromisoformat(value)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract one time window from postgres-source to S3 raw.")
    parser.add_argument("--start", required=True, type=parse_utc_timestamp, help="window start, inclusive (ISO 8601)")
    parser.add_argument("--end", required=True, type=parse_utc_timestamp, help="window end, exclusive (ISO 8601)")
    args = parser.parse_args()
    if args.start >= args.end:
        parser.error(f"--start ({args.start.isoformat()}) must be earlier than --end ({args.end.isoformat()})")
    return args


def fetch_window_rows(connection, table_name: str, watermark_column: str, window_start: datetime, window_end: datetime) -> list[dict]:
    with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
        cursor.execute(
            f"""
            SELECT * FROM {table_name}
            WHERE {watermark_column} >= %s
              AND {watermark_column} < %s
            ORDER BY {watermark_column}
            """,
            (window_start, window_end),
        )
        return cursor.fetchall()


def rows_to_jsonl(rows: list[dict]) -> str:
    """Convert rows to JSON Lines text -- one JSON object per line, the
    standard raw-landing format (append-friendly, schema-flexible)."""
    lines = []
    for row in rows:
        # default=str handles datetime/Decimal objects psycopg2 returns
        lines.append(json.dumps(dict(row), default=str))
    return "\n".join(lines)


def upload_to_s3(s3_client, table_name: str, run_id: str, jsonl_content: str) -> str:
    ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"raw/{table_name}/ingestion_date={ingestion_date}/run_id={run_id}/{table_name}.jsonl"
    s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=key, Body=jsonl_content.encode("utf-8"))
    return key


def extract_table(connection, s3_client, table_name: str, watermark_column: str, window_start: datetime, window_end: datetime, run_id: str) -> int:
    window_rows = fetch_window_rows(connection, table_name, watermark_column, window_start, window_end)

    if not window_rows:
        print(f"  {table_name}: no rows in window")
        return 0

    jsonl_content = rows_to_jsonl(window_rows)
    s3_key = upload_to_s3(s3_client, table_name, run_id, jsonl_content)
    print(f"  {table_name}: extracted {len(window_rows)} rows -> s3://{S3_BUCKET_NAME}/{s3_key}")
    return len(window_rows)


def main():
    args = parse_args()
    run_id = uuid.uuid4().hex[:12]
    print(f"Starting extract, run_id={run_id}, window=[{args.start.isoformat()}, {args.end.isoformat()})")

    connection = psycopg2.connect(**DB_CONFIG)
    s3_client = boto3.client("s3")

    try:
        total_rows = sum(
            extract_table(connection, s3_client, table_name, watermark_column, args.start, args.end, run_id)
            for table_name, watermark_column in TABLES.items()
        )
    finally:
        connection.close()

    print(f"Done. {total_rows} rows extracted across {len(TABLES)} tables.")


if __name__ == "__main__":
    main()
