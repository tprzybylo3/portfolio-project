"""
Incremental extract: postgres-source -> S3 raw (append-only).

For each table, reads the last watermark from a local state file, pulls
only rows newer than that watermark, writes them as JSON Lines, and
uploads to S3 under a path partitioned by ingestion date + run_id
(never overwriting anything -- see ADR-002).

Watermark state lives in a local JSON file for now. This is a deliberate
placeholder: state belongs to the PIPELINE, not to postgres-source's
schema. Once Airflow is introduced (Phase 6), this moves to Airflow
Variables/XCom instead of a local file -- for a standalone script today,
a local file is honest about the mechanics without over-building.

Usage:
    uv run python scripts/extract.py
"""

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import boto3
import psycopg2
import psycopg2.extras

# --- Connection settings matching docker-compose.yml ---
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "ecommerce",
    "user": "source_user",
    "password": "source_password",
}

# --- Change this to your actual bucket name if different ---
S3_BUCKET_NAME = "tp-portfolio-ecommerce-raw"

STATE_FILE = Path(__file__).parent / "state" / "watermark_state.json"

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

# Earliest possible watermark, used only on a table's very first run.
EPOCH_START = datetime(1970, 1, 1, tzinfo=timezone.utc)


def load_watermark_state() -> dict:
    if STATE_FILE.exists():
        with open(STATE_FILE) as f:
            return json.load(f)
    return {}


def save_watermark_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2, default=str)


def fetch_new_rows(connection, table_name: str, watermark_column: str, since: datetime) -> list[dict]:
    with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
        cursor.execute(
            f"""
            SELECT * FROM {table_name}
            WHERE {watermark_column} > %s
            ORDER BY {watermark_column}
            """,
            (since,),
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


def extract_table(connection, s3_client, table_name: str, watermark_column: str, watermark_state: dict, run_id: str):
    last_watermark_str = watermark_state.get(table_name)
    last_watermark = datetime.fromisoformat(last_watermark_str) if last_watermark_str else EPOCH_START

    new_rows = fetch_new_rows(connection, table_name, watermark_column, last_watermark)

    if not new_rows:
        print(f"  {table_name}: no new rows since {last_watermark.isoformat()}")
        return

    jsonl_content = rows_to_jsonl(new_rows)
    s3_key = upload_to_s3(s3_client, table_name, run_id, jsonl_content)

    new_watermark = max(row[watermark_column] for row in new_rows)
    watermark_state[table_name] = new_watermark.isoformat()

    print(f"  {table_name}: extracted {len(new_rows)} rows -> s3://{S3_BUCKET_NAME}/{s3_key}")
    print(f"    watermark advanced to {new_watermark.isoformat()}")


def main():
    run_id = uuid.uuid4().hex[:12]
    print(f"Starting incremental extract, run_id={run_id}")

    watermark_state = load_watermark_state()
    connection = psycopg2.connect(**DB_CONFIG)
    s3_client = boto3.client("s3")

    try:
        for table_name, watermark_column in TABLES.items():
            extract_table(connection, s3_client, table_name, watermark_column, watermark_state, run_id)
    finally:
        connection.close()

    save_watermark_state(watermark_state)
    print("Done. Watermark state saved.")


if __name__ == "__main__":
    main()
