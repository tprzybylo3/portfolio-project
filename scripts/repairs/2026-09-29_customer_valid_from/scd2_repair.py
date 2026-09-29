# One-off repair of event-time SCD2 boundaries on glue_catalog.portfolio_ecommerce.customer.
# Run: docker exec -i spark-iceberg python3 - <mode> [arg] < scd2_repair.py
#   snapshot            -> JSON state (count, metadata_location, snapshot_id, per-row dump) on stdout
#   dryrun              -> prints the corrections that apply would make
#   apply <ids|all>     -> MERGE (UPDATE-only) of corrections for comma-separated ids, or all remaining
# Never drops/recreates anything; corrections are derived from raw S3 extracts, not hardcoded.
import json
import sys

import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

GLUE_DATABASE = "portfolio_ecommerce"
TABLE = f"glue_catalog.{GLUE_DATABASE}.customer"
S3_BUCKET_NAME = "tp-portfolio-ecommerce-raw"
ATTRIBUTE_COLUMNS = ["first_name", "last_name", "email", "country"]
TS_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS"

spark = (
    SparkSession.builder.appName("customer-scd2-repair")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://{S3_BUCKET_NAME}/warehouse/")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


def table_state() -> dict:
    metadata_location = boto3.client("glue").get_table(DatabaseName=GLUE_DATABASE, Name="customer")["Table"]["Parameters"]["metadata_location"]
    snapshot_id = spark.sql(f"SELECT snapshot_id FROM {TABLE}.snapshots ORDER BY committed_at DESC LIMIT 1").collect()[0][0]
    rows = spark.sql(f"""
        SELECT customer_id,
               DATE_FORMAT(valid_from, '{TS_FORMAT}') AS valid_from,
               DATE_FORMAT(valid_to, '{TS_FORMAT}')   AS valid_to,
               is_current,
               SHA2(CONCAT_WS('|', {', '.join(ATTRIBUTE_COLUMNS)}), 256) AS attribute_hash
        FROM {TABLE}
        ORDER BY customer_id, valid_from
    """).collect()
    return {
        "row_count": len(rows),
        "metadata_location": metadata_location,
        "snapshot_id": str(snapshot_id),
        "rows": [r.asDict() for r in rows],
    }


def load_raw_customers():
    s3 = boto3.client("s3")
    records = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=S3_BUCKET_NAME, Prefix="raw/customers/"):
        for obj in page.get("Contents", []):
            body = s3.get_object(Bucket=S3_BUCKET_NAME, Key=obj["Key"])["Body"].read().decode()
            records += [json.loads(line) for line in body.splitlines() if line.strip()]
    raw = spark.createDataFrame([{k: str(r[k]) for k in ["customer_id", "created_at", "updated_at"] + ATTRIBUTE_COLUMNS} for r in records])
    return (
        raw.withColumn("customer_id", F.col("customer_id").cast("bigint"))
        .withColumn("created_at", F.to_timestamp("created_at"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
    )


def build_corrections():
    load_raw_customers().createOrReplaceTempView("raw_customers")
    attribute_match = " AND ".join(f"r.{c} = v.{c}" for c in ATTRIBUTE_COLUMNS)

    # Matching a version to raw rows by attribute values is only unambiguous if no
    # customer ever returns to an earlier attribute set (A -> B -> A). Fail loud if so.
    repeated_attribute_sets = spark.sql(f"""
        SELECT customer_id FROM {TABLE}
        GROUP BY customer_id, {', '.join(ATTRIBUTE_COLUMNS)} HAVING COUNT(*) > 1
    """).count()
    if repeated_attribute_sets:
        sys.exit(f"STOP: {repeated_attribute_sets} customer(s) repeat an attribute set across versions -- boundary derivation ambiguous")

    corrections = spark.sql(f"""
        WITH ordered_versions AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY valid_from) AS version_no
            FROM {TABLE}
        ),
        first_created AS (
            SELECT customer_id, MIN(created_at) AS first_created_at
            FROM raw_customers GROUP BY customer_id
        ),
        version_event_start AS (
            -- earliest raw change event that produced each version's attribute values
            SELECT v.customer_id, v.version_no, MIN(r.updated_at) AS event_start
            FROM ordered_versions v
            JOIN raw_customers r ON r.customer_id = v.customer_id AND {attribute_match}
            GROUP BY v.customer_id, v.version_no
        ),
        corrected_versions AS (
            SELECT
                v.customer_id,
                v.valid_from AS old_valid_from,
                v.valid_to   AS old_valid_to,
                CASE WHEN v.version_no = 1 THEN fc.first_created_at ELSE this_start.event_start END AS new_valid_from,
                CASE WHEN v.valid_to IS NULL THEN NULL ELSE next_start.event_start END              AS new_valid_to
            FROM ordered_versions v
            LEFT JOIN first_created fc          ON fc.customer_id = v.customer_id
            LEFT JOIN version_event_start this_start ON this_start.customer_id = v.customer_id AND this_start.version_no = v.version_no
            LEFT JOIN version_event_start next_start ON next_start.customer_id = v.customer_id AND next_start.version_no = v.version_no + 1
        )
        SELECT * FROM corrected_versions
        WHERE NOT (old_valid_from <=> new_valid_from) OR NOT (old_valid_to <=> new_valid_to)
    """).cache()

    invalid = corrections.filter(
        F.col("new_valid_from").isNull()
        | (F.col("old_valid_to").isNotNull() & F.col("new_valid_to").isNull())
        | (F.col("new_valid_to").isNotNull() & (F.col("new_valid_from") >= F.col("new_valid_to")))
    ).count()
    if invalid:
        sys.exit(f"STOP: {invalid} correction(s) are missing a value or would give valid_from >= valid_to")
    return corrections


def print_corrections(corrections):
    for r in corrections.orderBy("customer_id", "old_valid_from").select(
        "customer_id",
        *[F.date_format(c, TS_FORMAT).alias(c) for c in ["old_valid_from", "new_valid_from", "old_valid_to", "new_valid_to"]],
    ).collect():
        to_change = f"  valid_to {r.old_valid_to} -> {r.new_valid_to}" if r.old_valid_to != r.new_valid_to else ""
        print(f"  id={r.customer_id:>3}  valid_from {r.old_valid_from} -> {r.new_valid_from}{to_change}")


mode = sys.argv[1]
if mode == "snapshot":
    print(json.dumps(table_state(), indent=2))
elif mode == "dryrun":
    corrections = build_corrections()
    print(f"{corrections.count()} row(s) would change:")
    print_corrections(corrections)
elif mode == "apply":
    corrections = build_corrections()
    if sys.argv[2] != "all":
        corrections = corrections.filter(F.col("customer_id").isin([int(i) for i in sys.argv[2].split(",")]))
    print(f"applying {corrections.count()} row correction(s):")
    print_corrections(corrections)
    # Materialize before MERGE -- corrections are derived from the target table, and a lazy
    # view could re-evaluate against the post-MERGE state (same lesson as merge_dimensions.py).
    spark.createDataFrame(corrections.collect(), corrections.schema).createOrReplaceTempView("corrections")
    spark.sql(f"""
        MERGE INTO {TABLE} AS target
        USING corrections AS c
        ON target.customer_id = c.customer_id AND target.valid_from = c.old_valid_from
        WHEN MATCHED THEN UPDATE SET
            target.valid_from = c.new_valid_from,
            target.valid_to   = c.new_valid_to
    """)
    print("MERGE committed")
else:
    sys.exit(f"unknown mode {mode}")
spark.stop()
