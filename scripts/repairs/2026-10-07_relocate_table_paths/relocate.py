# Move an Iceberg table's files to the S3 directory matching its current Glue name
# (ADR-005 renamed the tables but left e.g. .../dim_product/). Keeps the full snapshot
# history: Iceberg rewrite_table_path + register_table, then a two-rename swap.
#
# Run (inside the spark-iceberg container, one table per call):
#   docker exec spark-iceberg python3 /opt/spark-scripts/repairs/2026-10-07_relocate_table_paths/relocate.py <mode> <table|all>
#     snapshot <table|all>  -> JSON state on stdout (row count, metadata_location, snapshots, content hash)
#     prepare  <table>      -> rewrite paths, copy files, register in the staging Glue DB, verify vs live
#                              (staging files under migration-staging/2026-10-07/<table>/<run id>/)
#     swap     <table>      -> re-verify, then live -> staging DB (<table>_old_path), staged -> live name
#     rollback <table>      -> the swap's two renames in reverse
#     check    <table|all>  -> read-only: live table references only its new directory
# Cleanup after the user's confirmation: cleanup.py (same directory).
#
# Never drops anything and never deletes S3 objects: old directories, the <table>_old_path
# Glue entries and the staging prefix stay until the user confirms removal.
# All temporary Glue entries live in STAGING_DATABASE, which Snowflake's catalog-linked
# database does not link, so no unpoliced copy of a table is ever exposed there.
import csv
import io
import json
import os
import sys
from datetime import datetime, timezone

import boto3
from pyspark.sql import SparkSession

GLUE_DATABASE = "portfolio_ecommerce"
STAGING_DATABASE = "portfolio_ecommerce_relocation"
S3_BUCKET_NAME = "tp-portfolio-ecommerce-raw"
DATABASE_PREFIX = f"s3://{S3_BUCKET_NAME}/warehouse/{GLUE_DATABASE}.db"
STAGING_LOCATION_PREFIX = f"s3://{S3_BUCKET_NAME}/migration-staging/2026-10-07"
# Each prepare gets its own staging subdirectory, so a failed run's leftovers never
# block or mix with the next one (all of migration-staging/ is removed at the end,
# on the user's confirmation).
RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
AWS_REGION = os.environ.get("AWS_REGION", "eu-central-1")
OLD_DIRECTORY_BY_TABLE = {
    "product": "dim_product",
    "order_items": "fact_order_items",
    "order_status_history": "fact_order_status_history",
    "payments": "fact_payments",
    "orders": "fact_orders",
    "customer": "dim_customer",
}
# Table properties that hold paths -- rewrite_table_path does not rewrite them.
PATH_PROPERTIES = ["write.data.path", "write.metadata.path", "write.object-storage.path", "write.folder-storage.path"]

spark = (
    SparkSession.builder.appName("relocate-table-paths")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://{S3_BUCKET_NAME}/warehouse/")
    # renames happen mid-run; a cached table object would keep pointing at the old entry
    .config("spark.sql.catalog.glue_catalog.cache-enabled", "false")
    # rewrite_table_path saves its file list through Hadoop FileSystem, not S3FileIO
    # (ai/failure-log/001), and the image has no Hadoop S3 filesystem. hadoop-aws is
    # pinned to the Hadoop version bundled with PySpark 3.5.5 (3.3.4) and mapped to the
    # s3:// scheme; credentials come from the container's AWS_* env vars.
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.endpoint", f"s3.{AWS_REGION}.amazonaws.com")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
glue = boto3.client("glue")
s3 = boto3.client("s3")


def stop(message: str):
    sys.exit(f"STOP: {message}")


def split_s3_uri(uri: str) -> tuple:
    bucket, _, key = uri.removeprefix("s3://").partition("/")
    return bucket, key


def glue_entry(database: str, table: str):
    try:
        return glue.get_table(DatabaseName=database, Name=table)["Table"]
    except glue.exceptions.EntityNotFoundException:
        return None


def table_state(database: str, table: str) -> dict:
    """Everything that must be identical before and after a move (except paths)."""
    name = f"glue_catalog.{database}.{table}"
    entry = glue_entry(database, table)
    snapshots = spark.sql(f"""
        SELECT snapshot_id, parent_id, operation, CAST(committed_at AS STRING) AS committed_at
        FROM {name}.snapshots ORDER BY committed_at, snapshot_id
    """).collect()
    current_snapshot_id = json.loads(read_s3_text(entry["Parameters"]["metadata_location"]))["current-snapshot-id"]
    # order-independent content fingerprint; DECIMAL avoids silent BIGINT overflow
    row_count, content_hash = spark.sql(f"SELECT COUNT(*), CAST(SUM(CAST(xxhash64(*) AS DECIMAL(38, 0))) AS STRING) FROM {name}").collect()[0]
    oldest_snapshot_id = snapshots[0].snapshot_id
    oldest_snapshot_row_count = spark.sql(f"SELECT COUNT(*) FROM {name} VERSION AS OF {oldest_snapshot_id}").collect()[0][0]
    return {
        "table": f"{database}.{table}",
        "row_count": row_count,
        "content_hash": content_hash,
        "current_snapshot_id": str(current_snapshot_id),
        "snapshot_count": len(snapshots),
        "snapshots": [{k: (str(v) if v is not None else None) for k, v in s.asDict().items()} for s in snapshots],
        "oldest_snapshot_row_count": oldest_snapshot_row_count,
        "metadata_location": entry["Parameters"]["metadata_location"],
        "glue_location": entry.get("StorageDescriptor", {}).get("Location"),
    }


# Fields that must match between the live table and its relocated copy.
COMPARED_FIELDS = ["row_count", "content_hash", "current_snapshot_id", "snapshot_count", "snapshots", "oldest_snapshot_row_count"]


def state_mismatches(expected: dict, actual: dict) -> list:
    return [f for f in COMPARED_FIELDS if expected[f] != actual[f]]


def referenced_paths(database: str, table: str) -> list:
    """Every path the table's metadata points at: data/delete files, manifests,
    manifest lists, previous metadata files, and the table location itself."""
    name = f"glue_catalog.{database}.{table}"
    queries = [
        f"SELECT file_path AS path FROM {name}.all_files",
        f"SELECT path FROM {name}.all_manifests",
        f"SELECT manifest_list AS path FROM {name}.snapshots",
        f"SELECT file AS path FROM {name}.metadata_log_entries",
    ]
    paths = [r.path for q in queries for r in spark.sql(q).collect()]
    current_metadata = read_s3_text(glue_entry(database, table)["Parameters"]["metadata_location"])
    metadata = json.loads(current_metadata)
    paths.append(metadata["location"])
    paths += [s["statistics-path"] for s in metadata.get("statistics", [])]
    paths += [s["statistics-path"] for s in metadata.get("partition-statistics", [])]
    return paths


def read_s3_text(uri: str) -> str:
    bucket, key = split_s3_uri(uri)
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode()


def read_file_list(uri: str) -> list:
    """rewrite_table_path's file_list_location is a Spark CSV output directory
    (part-*.csv + _SUCCESS), not a single file: (source, target) pairs from all parts."""
    bucket, key = split_s3_uri(uri.rstrip("/") + "/")
    part_keys = [
        o["Key"]
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=key)
        for o in page.get("Contents", [])
        if o["Key"].rsplit("/", 1)[-1].startswith("part-") and o["Key"].endswith(".csv")
    ]
    if not part_keys:
        stop(f"no part-*.csv files under {uri}")
    return [row for k in part_keys for row in csv.reader(io.StringIO(read_s3_text(f"s3://{bucket}/{k}"))) if row]


def prefix_is_empty(uri: str) -> bool:
    bucket, key = split_s3_uri(uri.rstrip("/") + "/")
    return s3.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1)["KeyCount"] == 0


def paths_for(table: str) -> tuple:
    if table not in OLD_DIRECTORY_BY_TABLE:
        stop(f"unknown table {table}")
    return f"{DATABASE_PREFIX}/{OLD_DIRECTORY_BY_TABLE[table]}", f"{DATABASE_PREFIX}/{table}"


def prepare(table: str):
    source_prefix, target_prefix = paths_for(table)
    live_name = f"glue_catalog.{GLUE_DATABASE}.{table}"
    staging_location = f"{STAGING_LOCATION_PREFIX}/{table}/{RUN_ID}/"

    # --- pre-checks: nothing below may overwrite or reuse anything
    if glue_entry(STAGING_DATABASE, table) is not None:
        stop(f"{STAGING_DATABASE}.{table} already exists")
    if not prefix_is_empty(target_prefix):
        stop(f"target {target_prefix}/ is not empty")
    if not prefix_is_empty(staging_location):
        stop(f"staging location {staging_location} is not empty")
    properties = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {live_name}").collect()}
    path_properties = {k: properties[k] for k in PATH_PROPERTIES if k in properties}
    if path_properties:
        stop(f"table has path-valued properties rewrite_table_path would not touch: {path_properties}")
    outside_source = [p for p in referenced_paths(GLUE_DATABASE, table) if not p.startswith(source_prefix + "/") and p != source_prefix]
    if outside_source:
        stop(f"{len(outside_source)} referenced path(s) outside {source_prefix}, e.g. {outside_source[:3]}")

    live_state = table_state(GLUE_DATABASE, table)
    print(f"live: {live_state['row_count']} rows, {live_state['snapshot_count']} snapshots, current {live_state['current_snapshot_id']}")

    # --- 1. rewrite metadata with the new prefix (writes only under staging_location)
    rewrite = spark.sql(f"""
        CALL glue_catalog.system.rewrite_table_path(
            table => '{GLUE_DATABASE}.{table}',
            source_prefix => '{source_prefix}',
            target_prefix => '{target_prefix}',
            staging_location => '{staging_location}'
        )
    """).collect()[0]
    print(f"rewrite_table_path: latest_version={rewrite.latest_version} file_list={rewrite.file_list_location}")

    # --- 2. copy every listed file (rewritten metadata from staging, data files from source)
    copy_pairs = read_file_list(rewrite.file_list_location)
    if any(not target.startswith(target_prefix + "/") for _, target in copy_pairs):
        stop("file list contains a target outside the target prefix")
    for source, target in copy_pairs:
        source_bucket, source_key = split_s3_uri(source)
        target_bucket, target_key = split_s3_uri(target)
        s3.copy({"Bucket": source_bucket, "Key": source_key}, target_bucket, target_key)
    size_mismatches = [
        target for source, target in copy_pairs
        if s3.head_object(Bucket=split_s3_uri(source)[0], Key=split_s3_uri(source)[1])["ContentLength"]
        != s3.head_object(Bucket=split_s3_uri(target)[0], Key=split_s3_uri(target)[1])["ContentLength"]
    ]
    if size_mismatches:
        stop(f"{len(size_mismatches)} copied file(s) differ in size, e.g. {size_mismatches[:3]}")
    print(f"copied {len(copy_pairs)} file(s), sizes verified")

    # --- 3. register the relocated copy in the staging Glue DB (invisible to Snowflake)
    latest_metadata = [t for _, t in copy_pairs if t.endswith("/" + rewrite.latest_version.split("/")[-1])]
    if len(latest_metadata) != 1:
        stop(f"expected exactly one copied file for {rewrite.latest_version}, found {latest_metadata}")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS glue_catalog.{STAGING_DATABASE}")
    registered = spark.sql(f"""
        CALL glue_catalog.system.register_table(
            table => '{STAGING_DATABASE}.{table}',
            metadata_file => '{latest_metadata[0]}'
        )
    """).collect()[0]
    print(f"registered {STAGING_DATABASE}.{table}: snapshot {registered.current_snapshot_id}, {registered.total_records_count} records")

    # --- 4. verify the copy against the live table
    staged_state = table_state(STAGING_DATABASE, table)
    mismatches = state_mismatches(live_state, staged_state)
    if mismatches:
        stop(f"relocated copy differs from live table in {mismatches}")
    staged_paths = referenced_paths(STAGING_DATABASE, table)
    still_old = [p for p in staged_paths if p.startswith(source_prefix + "/") or p == source_prefix]
    not_new = [p for p in staged_paths if not (p.startswith(target_prefix + "/") or p == target_prefix)]
    if still_old or not_new:
        stop(f"relocated copy still references other paths: old={still_old[:3]} other={not_new[:3]}")
    print(f"verified: {', '.join(COMPARED_FIELDS)} identical; all {len(staged_paths)} referenced paths under {target_prefix}/")
    print(json.dumps({"live": live_state, "staged": staged_state}, indent=2))


def rename(from_database: str, from_table: str, to_database: str, to_table: str):
    spark.sql(f"ALTER TABLE glue_catalog.{from_database}.{from_table} RENAME TO {to_database}.{to_table}")
    if glue_entry(from_database, from_table) is not None or glue_entry(to_database, to_table) is None:
        stop(f"rename {from_database}.{from_table} -> {to_database}.{to_table} did not land as expected")


def swap(table: str):
    _, target_prefix = paths_for(table)
    if table == "customer" and "--governance-ack" not in sys.argv:
        stop("customer carries masking/RLS in Snowflake (CLAUDE.md decision 6) -- pass --governance-ack once the user is ready to re-attach them")
    if glue_entry(STAGING_DATABASE, f"{table}_old_path") is not None:
        stop(f"{STAGING_DATABASE}.{table}_old_path already exists")

    # the live table must not have changed since prepare
    live_state = table_state(GLUE_DATABASE, table)
    staged_state = table_state(STAGING_DATABASE, table)
    mismatches = state_mismatches(live_state, staged_state)
    if mismatches:
        stop(f"live and staged differ in {mismatches} (live written since prepare?) -- re-run prepare")

    rename(GLUE_DATABASE, table, STAGING_DATABASE, f"{table}_old_path")
    rename(STAGING_DATABASE, table, GLUE_DATABASE, table)

    after_state = table_state(GLUE_DATABASE, table)
    mismatches = state_mismatches(live_state, after_state)
    if mismatches:
        stop(f"after swap {GLUE_DATABASE}.{table} differs in {mismatches} -- run rollback")
    if not after_state["metadata_location"].startswith(target_prefix + "/metadata/"):
        stop(f"after swap metadata_location is {after_state['metadata_location']} -- run rollback")
    print(f"swapped: {GLUE_DATABASE}.{table} now at {after_state['metadata_location']}")
    print(json.dumps({"before": live_state, "after": after_state}, indent=2))


def check(table: str):
    """Read-only: the live table references nothing outside its new directory
    (precondition for deleting the old one)."""
    source_prefix, target_prefix = paths_for(table)
    paths = referenced_paths(GLUE_DATABASE, table)
    still_old = [p for p in paths if p.startswith(source_prefix + "/") or p == source_prefix]
    not_new = [p for p in paths if not (p.startswith(target_prefix + "/") or p == target_prefix)]
    if still_old or not_new:
        stop(f"{table} still references other paths: old={still_old[:3]} other={not_new[:3]}")
    print(f"ok: {table} -- all {len(paths)} referenced paths under {target_prefix}/")


def rollback(table: str):
    if glue_entry(STAGING_DATABASE, f"{table}_old_path") is None:
        stop(f"{STAGING_DATABASE}.{table}_old_path not found -- nothing to roll back")
    rename(GLUE_DATABASE, table, STAGING_DATABASE, table)
    rename(STAGING_DATABASE, f"{table}_old_path", GLUE_DATABASE, table)
    print(f"rolled back: {GLUE_DATABASE}.{table} at {glue_entry(GLUE_DATABASE, table)['Parameters']['metadata_location']}")


mode, target = sys.argv[1], sys.argv[2]
if mode == "snapshot":
    tables = list(OLD_DIRECTORY_BY_TABLE) if target == "all" else [target]
    print(json.dumps([table_state(GLUE_DATABASE, t) for t in tables], indent=2))
elif mode == "prepare":
    prepare(target)
elif mode == "swap":
    swap(target)
elif mode == "rollback":
    rollback(target)
elif mode == "check":
    for t in (list(OLD_DIRECTORY_BY_TABLE) if target == "all" else [target]):
        check(t)
else:
    stop(f"unknown mode {mode}")
spark.stop()
