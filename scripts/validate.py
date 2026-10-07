"""
Phase 2: lightweight, custom data quality validation on the raw data
landed in S3 by extract.py -- schema checks, null checks, PK uniqueness
(within the batch), accepted values, and quarantine for rows that fail.

Deliberately simple, hand-written Python -- NOT Great Expectations (that's
Advanced, layered on top of these same checkpoints later, not a
replacement). Runs AFTER extraction, BEFORE the data would become a
candidate for the PySpark MERGE into Iceberg (see ADR-002).

For each table, finds the MOST RECENTLY landed file in S3 (by
LastModified, not by guessing run_id), validates every row, and uploads
any failing rows to a parallel "quarantine" prefix with a machine-readable
reason attached -- nothing is silently dropped.

Usage:
    uv run python scripts/validate.py
"""

import json
import uuid
from datetime import datetime, timezone

import boto3

S3_BUCKET_NAME = "tp-portfolio-ecommerce-raw"

# Per-table expectations. Deliberately explicit and readable over clever --
# this is the kind of thing that gets read out loud in an interview.
TABLE_EXPECTATIONS = {
    "customers": {
        "required_columns": {"customer_id", "first_name", "last_name", "email", "country", "created_at", "updated_at"},
        "not_null_columns": {"customer_id", "email", "country"},
        "pk_column": "customer_id",
        "accepted_values": {},
    },
    "products": {
        "required_columns": {"product_id", "name", "category", "price", "stock_qty", "created_at", "updated_at"},
        "not_null_columns": {"product_id", "name", "price"},
        "pk_column": "product_id",
        "accepted_values": {},
    },
    "orders": {
        "required_columns": {"order_id", "customer_id", "status", "order_date", "created_at", "updated_at"},
        "not_null_columns": {"order_id", "customer_id", "status"},
        "pk_column": "order_id",
        "accepted_values": {"status": {"pending", "paid", "shipped", "delivered", "cancelled"}},
    },
    "order_items": {
        "required_columns": {"order_item_id", "order_id", "product_id", "qty", "unit_price", "created_at", "updated_at"},
        "not_null_columns": {"order_item_id", "order_id", "product_id", "qty", "unit_price"},
        "pk_column": "order_item_id",
        "accepted_values": {},
    },
    "payments": {
        "required_columns": {"payment_id", "order_id", "amount", "method", "status", "created_at", "updated_at"},
        "not_null_columns": {"payment_id", "order_id", "amount", "method", "status"},
        "pk_column": "payment_id",
        "accepted_values": {
            "method": {"credit_card", "debit_card", "paypal", "bank_transfer"},
            "status": {"pending", "completed", "failed", "refunded"},
        },
    },
    "order_status_history": {
        "required_columns": {"history_id", "order_id", "status", "changed_at"},
        "not_null_columns": {"history_id", "order_id", "status"},
        "pk_column": "history_id",
        "accepted_values": {"status": {"pending", "paid", "shipped", "delivered", "cancelled"}},
    },
}


def find_latest_object_key(s3_client, table_name: str) -> str | None:
    """List everything landed for this table and return the key of the
    most recently uploaded file, by actual S3 LastModified -- not by
    parsing the run_id/date out of the key path."""
    prefix = f"raw/{table_name}/"
    paginator = s3_client.get_paginator("list_objects_v2")
    latest_key, latest_modified = None, None

    for page in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=prefix):
        for obj in page.get("Contents", []):
            if latest_modified is None or obj["LastModified"] > latest_modified:
                latest_key, latest_modified = obj["Key"], obj["LastModified"]

    return latest_key


def load_jsonl_from_s3(s3_client, key: str) -> list[dict]:
    body = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=key)["Body"].read().decode("utf-8")
    if not body.strip():
        return []
    return [json.loads(line) for line in body.strip().split("\n")]


def validate_row(row: dict, expectations: dict) -> list[str]:
    """Returns a list of failure reasons -- empty list means the row passed."""
    failures = []

    missing_columns = expectations["required_columns"] - row.keys()
    if missing_columns:
        failures.append(f"missing_columns:{sorted(missing_columns)}")
        # can't meaningfully run the rest of the checks without the columns
        return failures

    for column in expectations["not_null_columns"]:
        if row.get(column) is None:
            failures.append(f"null_value:{column}")

    for column, allowed_values in expectations["accepted_values"].items():
        value = row.get(column)
        if value is not None and value not in allowed_values:
            failures.append(f"unexpected_value:{column}={value!r}")

    return failures


def check_pk_uniqueness(rows: list[dict], pk_column: str) -> set:
    """Returns the set of PK values that appear more than once in this batch."""
    seen, duplicates = set(), set()
    for row in rows:
        pk_value = row.get(pk_column)
        if pk_value in seen:
            duplicates.add(pk_value)
        seen.add(pk_value)
    return duplicates


def upload_quarantine(s3_client, table_name: str, run_id: str, quarantined_rows: list[dict]) -> str | None:
    if not quarantined_rows:
        return None
    ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"quarantine/{table_name}/ingestion_date={ingestion_date}/run_id={run_id}/{table_name}_quarantine.jsonl"
    content = "\n".join(json.dumps(row) for row in quarantined_rows)
    s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=key, Body=content.encode("utf-8"))
    return key


def validate_table(s3_client, table_name: str, expectations: dict, run_id: str):
    source_key = find_latest_object_key(s3_client, table_name)
    if source_key is None:
        print(f"  {table_name}: nothing landed in S3 yet, skipping")
        return

    rows = load_jsonl_from_s3(s3_client, source_key)
    if not rows:
        print(f"  {table_name}: latest file ({source_key}) is empty, nothing to validate")
        return

    duplicate_pks = check_pk_uniqueness(rows, expectations["pk_column"])

    quarantined = []
    valid_count = 0
    for row in rows:
        failures = validate_row(row, expectations)
        pk_value = row.get(expectations["pk_column"])
        if pk_value in duplicate_pks:
            failures.append(f"duplicate_pk:{expectations['pk_column']}={pk_value!r}")

        if failures:
            quarantined.append({**row, "_quarantine_reasons": failures})
        else:
            valid_count += 1

    quarantine_key = upload_quarantine(s3_client, table_name, run_id, quarantined)

    status_line = f"  {table_name}: {len(rows)} rows checked, {valid_count} valid, {len(quarantined)} quarantined"
    if quarantine_key:
        status_line += f" -> s3://{S3_BUCKET_NAME}/{quarantine_key}"
    print(status_line)


def main():
    run_id = uuid.uuid4().hex[:12]
    print(f"Starting validation run_id={run_id}")

    s3_client = boto3.client("s3")
    for table_name, expectations in TABLE_EXPECTATIONS.items():
        validate_table(s3_client, table_name, expectations, run_id)

    print("Done.")


if __name__ == "__main__":
    main()
