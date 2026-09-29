"""
Phase 3: MERGE orders / order_items / payments / order_status_history into
Iceberg tables via AWS Glue Data Catalog.

Unlike customer/product, these do NOT get full SCD2 treatment --
deliberately. In Kimball terms these are FACTS, not dimensions: they
represent events/transactions, not slowly-changing descriptive attributes.
Two simpler patterns cover them:

- UPSERT (orders, order_items, payments): keep only the CURRENT state per
  business key, no valid_from/valid_to history. orders.status does
  change over time (pending -> paid -> shipped...), but the full history
  of that change already lives in order_status_history (Postgres itself
  tracks it as an append-only log, see ddl.sql) -- duplicating that
  history again here as SCD2 would be redundant.
- APPEND-ONLY (order_status_history): rows here are NEVER updated by
  design (ddl.sql has no trigger/UPDATE path for this table) -- so MERGE
  only ever needs a "insert if not already present" (dedup by PK),
  never an UPDATE branch at all.

One generic, config-driven script instead of four near-identical files --
each table's config below says its Iceberg column types, primary key,
partition column, and whether it's append-only.

Usage (run inside the spark-iceberg container):
    docker exec -it spark-iceberg python3 /opt/spark-scripts/merge_facts.py
"""

import os
import tempfile

import boto3
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F

S3_BUCKET_NAME = "tp-portfolio-ecommerce-raw"
GLUE_DATABASE = "portfolio_ecommerce"
WAREHOUSE_PATH = f"s3://{S3_BUCKET_NAME}/warehouse/"

TABLES = {
    "orders": {
        "iceberg_table": "orders",
        "pk": "order_id",
        "watermark_column": "updated_at",
        "partition_column": "created_at",
        "append_only": False,
        "columns": {
            "order_id": "BIGINT", "customer_id": "BIGINT", "status": "STRING",
            "order_date": "TIMESTAMP", "created_at": "TIMESTAMP", "updated_at": "TIMESTAMP",
        },
    },
    "order_items": {
        "iceberg_table": "order_items",
        "pk": "order_item_id",
        "watermark_column": "updated_at",
        "partition_column": "created_at",
        "append_only": False,
        "columns": {
            "order_item_id": "BIGINT", "order_id": "BIGINT", "product_id": "BIGINT",
            "qty": "INT", "unit_price": "DECIMAL(10,2)",
            "created_at": "TIMESTAMP", "updated_at": "TIMESTAMP",
        },
    },
    "payments": {
        "iceberg_table": "payments",
        "pk": "payment_id",
        "watermark_column": "updated_at",
        "partition_column": "created_at",
        "append_only": False,
        "columns": {
            "payment_id": "BIGINT", "order_id": "BIGINT", "amount": "DECIMAL(10,2)",
            "method": "STRING", "status": "STRING",
            "created_at": "TIMESTAMP", "updated_at": "TIMESTAMP",
        },
    },
    "order_status_history": {
        "iceberg_table": "order_status_history",
        "pk": "history_id",
        "watermark_column": "changed_at",  # this table has no updated_at -- see ddl.sql
        "partition_column": "changed_at",
        "append_only": True,
        "columns": {
            "history_id": "BIGINT", "order_id": "BIGINT",
            "status": "STRING", "changed_at": "TIMESTAMP",
        },
    },
}


def list_all_raw_keys(s3_client, table_name: str) -> list[str]:
    # Lists EVERY object key under raw/<table_name>/ in S3 -- not just the
    # newest one. Returns an empty list if the table has no raw data yet.
    prefix = f"raw/{table_name}/"
    paginator = s3_client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def download_all_to_local_dir(s3_client, keys: list[str]) -> str:
    # Downloads every given S3 key into ONE fresh local temp directory,
    # so Spark can read the whole directory as a single DataFrame at once.
    local_dir = tempfile.mkdtemp()
    for i, key in enumerate(keys):
        s3_client.download_file(S3_BUCKET_NAME, key, os.path.join(local_dir, f"batch_{i}.jsonl"))
    return local_dir


def build_spark_session() -> SparkSession:
    # Builds a SparkSession wired up to read/write Iceberg tables through
    # AWS Glue Data Catalog, with S3 as the underlying storage.
    return (
        SparkSession.builder
        .appName("merge-facts")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.glue_catalog.warehouse", WAREHOUSE_PATH)
        .getOrCreate()
    )


def ensure_table_exists(spark, config: dict):
    # Creates the Iceberg table for this fact if it doesn't already exist.
    # Columns come straight from config["columns"] -- no extra SCD2
    # columns here (facts don't get valid_from/valid_to/is_current,
    # unlike merge_dimensions.py). Partitioned by days() of whichever
    # date-like column the config names.
    iceberg_table = f"glue_catalog.{GLUE_DATABASE}.{config['iceberg_table']}"
    columns_sql = ",\n            ".join(f"{name} {dtype}" for name, dtype in config["columns"].items())
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {iceberg_table} (
            {columns_sql}
        )
        USING iceberg
        PARTITIONED BY (days({config['partition_column']}))
    """)


def merge_table(spark, table_name: str, config: dict, local_dir: str):
    # Loads + dedupes the incoming batch by PK, then runs ONE MERGE:
    # - append_only tables (order_status_history) only ever INSERT new
    #   rows (WHEN NOT MATCHED) -- there's no UPDATE branch at all, since
    #   these rows never change once written.
    # - everything else gets a plain upsert: overwrite the row's columns
    #   if the key already exists, insert it if it doesn't. No history
    #   kept (that's the whole point vs merge_dimensions.py's SCD2).
    iceberg_table = f"glue_catalog.{GLUE_DATABASE}.{config['iceberg_table']}"
    pk = config["pk"]
    columns = list(config["columns"].keys())

    source_df = spark.read.json(f"file://{local_dir}")

    # Spark's JSON reader infers STRING for date/time fields (JSON has no
    # native timestamp type). Iceberg's write path validates types
    # strictly (no implicit STRING -> TIMESTAMP cast), so cast every
    # TIMESTAMP-typed column from the config explicitly before it's used
    # for dedup ordering or written into the table.
    for col_name, col_type in config["columns"].items():
        if col_type == "TIMESTAMP":
            source_df = source_df.withColumn(col_name, F.to_timestamp(F.col(col_name)))
        elif col_type.startswith("DECIMAL"):
            # extract.py's default=str serializes Decimal to a JSON
            # string too (e.g. order_items.unit_price, payments.amount),
            # same reasoning as the TIMESTAMP cast above.
            source_df = source_df.withColumn(col_name, F.col(col_name).cast(col_type))

    # Dedup the incoming batch by PK (keep latest by watermark column) --
    # same defensive reasoning as merge_customers.py: MERGE doesn't
    # dedupe its own source.
    dedup_window = Window.partitionBy(pk).orderBy(F.col(config["watermark_column"]).desc())
    source_df = (
        source_df
        .withColumn("_row_num", F.row_number().over(dedup_window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )
    source_df.createOrReplaceTempView("source_batch")
    print(f"  {table_name}: incoming batch (deduped) = {source_df.count()} rows")

    insert_cols_sql = ", ".join(columns)
    insert_vals_sql = ", ".join(f"source.{c}" for c in columns)

    if config["append_only"]:
        # No UPDATE branch at all -- these rows never change once written.
        merge_sql = f"""
            MERGE INTO {iceberg_table} AS target
            USING source_batch AS source
            ON target.{pk} = source.{pk}
            WHEN NOT MATCHED THEN INSERT ({insert_cols_sql}) VALUES ({insert_vals_sql})
        """
    else:
        update_set_sql = ", ".join(f"target.{c} = source.{c}" for c in columns if c != pk)
        merge_sql = f"""
            MERGE INTO {iceberg_table} AS target
            USING source_batch AS source
            ON target.{pk} = source.{pk}
            WHEN MATCHED THEN UPDATE SET {update_set_sql}
            WHEN NOT MATCHED THEN INSERT ({insert_cols_sql}) VALUES ({insert_vals_sql})
        """

    spark.sql(merge_sql)

    total = spark.sql(f"SELECT COUNT(*) AS c FROM {iceberg_table}").collect()[0]["c"]
    print(f"  {table_name}: {config['iceberg_table']} now has {total} total rows")


def main():
    # Entry point: for each configured fact table, pull all its raw S3
    # files and run its merge. Skips a table cleanly if no raw data
    # exists yet, rather than erroring.
    s3_client = boto3.client("s3")
    spark = build_spark_session()

    for table_name, config in TABLES.items():
        raw_keys = list_all_raw_keys(s3_client, table_name)
        if not raw_keys:
            print(f"  {table_name}: no raw data in S3 yet, skipping")
            continue

        local_dir = download_all_to_local_dir(s3_client, raw_keys)
        ensure_table_exists(spark, config)
        merge_table(spark, table_name, config, local_dir)

    spark.stop()
    print("Done.")


if __name__ == "__main__":
    main()
