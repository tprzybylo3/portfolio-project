# Cleanup after relocate.py, run only on the user's explicit confirmation (2026-10-07).
# Run: docker exec spark-iceberg python3 /opt/spark-scripts/repairs/2026-10-07_relocate_table_paths/cleanup.py <mode>
#   listing            -> JSON of every object under warehouse/ and migration-staging/ (before/after evidence)
#   drop-old-entries   -> delete the 6 <table>_old_path Glue entries, then the empty staging database
#   delete-old-files   -> delete the 6 old table directories and migration-staging/
#
# Glue: glue.delete_table removes only the catalog entry -- the equivalent of a non-purge
# DROP TABLE, no S3 object is touched. S3: the bucket is not versioned, so deletion is
# permanent; precondition is `relocate.py check all` passing (no live table references
# an old directory). Prefixes are fixed below and end with "/", so e.g. fact_orders/
# cannot match fact_order_items/.
import json
import sys

import boto3

GLUE_DATABASE = "portfolio_ecommerce"
STAGING_DATABASE = "portfolio_ecommerce_relocation"
S3_BUCKET_NAME = "tp-portfolio-ecommerce-raw"
OLD_DIRECTORY_BY_TABLE = {
    "product": "dim_product",
    "order_items": "fact_order_items",
    "order_status_history": "fact_order_status_history",
    "payments": "fact_payments",
    "orders": "fact_orders",
    "customer": "dim_customer",
}
PREFIXES_TO_DELETE = [f"warehouse/{GLUE_DATABASE}.db/{d}/" for d in OLD_DIRECTORY_BY_TABLE.values()] + ["migration-staging/"]
LISTED_PREFIXES = ["warehouse/", "migration-staging/"]

glue = boto3.client("glue")
s3 = boto3.client("s3")


def stop(message: str):
    sys.exit(f"STOP: {message}")


def list_keys(prefix: str) -> list:
    return [
        {"key": o["Key"], "size": o["Size"]}
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=S3_BUCKET_NAME, Prefix=prefix)
        for o in page.get("Contents", [])
    ]


def listing():
    objects = {p: list_keys(p) for p in LISTED_PREFIXES}
    summary = {}
    for o in objects["warehouse/"]:
        directory = "/".join(o["key"].split("/")[:3])
        summary[directory] = summary.get(directory, 0) + 1
    summary["migration-staging"] = len(objects["migration-staging/"])
    print(json.dumps({"summary_object_counts": summary, "objects": objects}, indent=2))


def drop_old_entries():
    for table in OLD_DIRECTORY_BY_TABLE:
        name = f"{table}_old_path"
        entry = glue.get_table(DatabaseName=STAGING_DATABASE, Name=name)["Table"]
        # only ever remove an entry that still points at the OLD directory
        if f".db/{OLD_DIRECTORY_BY_TABLE[table]}/metadata/" not in entry["Parameters"]["metadata_location"]:
            stop(f"{STAGING_DATABASE}.{name} points at {entry['Parameters']['metadata_location']}, not the old directory")
        live = glue.get_table(DatabaseName=GLUE_DATABASE, Name=table)["Table"]["Parameters"]["metadata_location"]
        if f".db/{table}/metadata/" not in live:
            stop(f"live {GLUE_DATABASE}.{table} is at {live}, not the new directory")
        glue.delete_table(DatabaseName=STAGING_DATABASE, Name=name)
        print(f"removed Glue entry {STAGING_DATABASE}.{name} (catalog only, files untouched)")
    remaining = glue.get_tables(DatabaseName=STAGING_DATABASE)["TableList"]
    if remaining:
        stop(f"{STAGING_DATABASE} not empty: {[t['Name'] for t in remaining]}")
    glue.delete_database(Name=STAGING_DATABASE)
    print(f"removed empty Glue database {STAGING_DATABASE}")


def delete_old_files():
    if any(db["Name"] == STAGING_DATABASE for db in glue.get_databases()["DatabaseList"]):
        stop(f"{STAGING_DATABASE} still exists -- run drop-old-entries first")
    for prefix in PREFIXES_TO_DELETE:
        keys = [o["key"] for o in list_keys(prefix)]
        for batch_start in range(0, len(keys), 1000):
            batch = keys[batch_start:batch_start + 1000]
            response = s3.delete_objects(Bucket=S3_BUCKET_NAME, Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True})
            if response.get("Errors"):
                stop(f"delete errors under {prefix}: {response['Errors'][:3]}")
        left = len(list_keys(prefix))
        if left:
            stop(f"{left} object(s) still under {prefix}")
        print(f"deleted {len(keys)} object(s) under s3://{S3_BUCKET_NAME}/{prefix}")


mode = sys.argv[1]
if mode == "listing":
    listing()
elif mode == "drop-old-entries":
    drop_old_entries()
elif mode == "delete-old-files":
    delete_old_files()
else:
    stop(f"unknown mode {mode}")
