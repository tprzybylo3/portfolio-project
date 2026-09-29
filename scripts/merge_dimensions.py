"""
Phase 3: MERGE dimension batches (customers, products) into Iceberg
tables via AWS Glue Data Catalog -- full SCD Type 2 history for each.

One generic, config-driven script instead of two near-identical files
(the earlier merge_customers.py / merge_products.py) -- both dimensions
share the EXACT same SCD2 algorithm, differing only in column names/
types, so this is a case where consolidating genuinely simplifies things
(contrast with merge_facts.py, which stays separate because it's a
DIFFERENT algorithm -- simple upsert/append, not SCD2 -- not just
different column names; see that file's docstring).

Design notes carried over from the original merge_customers.py (found
via live debugging, not guessed up front):
- Reads ALL raw files per table, not just the latest -- incremental
  extraction produces many small delta files over time.
- Deduplicates the incoming batch by PK (latest watermark wins) BEFORE
  MERGE -- MERGE does not dedupe its own source.
- changed_ids is materialized via .collect() into a plain Python list
  BEFORE running MERGE -- a lazy SQL temp view would silently
  re-evaluate against the POST-MERGE table state (already closed rows)
  and return nothing.
- valid_from/valid_to use source.updated_at (EVENT time) rather than
  current_timestamp() (PROCESSING time), so history reflects when the
  change actually happened in the business, not when this script ran.

Usage (run inside the spark-iceberg container):
    docker exec -it spark-iceberg python3 /opt/spark-scripts/merge_dimensions.py
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

DIMENSIONS = {
    "customers": {
        "iceberg_table": "customer",
        "pk": "customer_id",
        "pk_type": "BIGINT",
        "watermark_column": "updated_at",
        "attribute_columns": {
            "first_name": "STRING",
            "last_name": "STRING",
            "email": "STRING",
            "country": "STRING",
        },
    },
    "products": {
        "iceberg_table": "product",
        "pk": "product_id",
        "pk_type": "BIGINT",
        "watermark_column": "updated_at",
        "attribute_columns": {
            "name": "STRING",
            "category": "STRING",
            "price": "DECIMAL(10,2)",
            "stock_qty": "INT",
        },
    },
}


def list_all_raw_keys(s3_client, table_name: str) -> list[str]:
    # Lists EVERY object key under raw/<table_name>/ in S3 -- not just the
    # newest one. Returns an empty list if the table has no raw data yet
    # (caller decides whether that's an error or just "skip for now").
    prefix = f"raw/{table_name}/"
    paginator = s3_client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def download_all_to_local_dir(s3_client, keys: list[str]) -> str:
    # Downloads every given S3 key into ONE fresh local temp directory,
    # so Spark can later read the whole directory as a single DataFrame
    # in one call, instead of downloading+reading+unioning file by file.
    local_dir = tempfile.mkdtemp()
    for i, key in enumerate(keys):
        s3_client.download_file(S3_BUCKET_NAME, key, os.path.join(local_dir, f"batch_{i}.jsonl"))
    return local_dir


def build_spark_session() -> SparkSession:
    # Builds a SparkSession wired up to read/write Iceberg tables through
    # AWS Glue Data Catalog (glue_catalog), with S3 as the underlying
    # storage for both table data and metadata.
    return (
        SparkSession.builder
        .appName("merge-dimensions")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.glue_catalog.warehouse", WAREHOUSE_PATH)
        .getOrCreate()
    )


def ensure_table_exists(spark, config: dict):
    # Creates the Iceberg table for this dimension if it doesn't already
    # exist (no-op if it does -- IF NOT EXISTS). Columns come from the
    # config's attribute_columns, plus the fixed SCD2 columns every
    # dimension gets: valid_from/valid_to/is_current. Partitioned by
    # days(valid_from) -- Iceberg "hidden partitioning" (cheat sheet 15.1).
    iceberg_table = f"glue_catalog.{GLUE_DATABASE}.{config['iceberg_table']}"
    attr_cols_sql = ",\n            ".join(f"{name} {dtype}" for name, dtype in config["attribute_columns"].items())
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {iceberg_table} (
            {config['pk']} {config['pk_type']},
            {attr_cols_sql},
            valid_from      TIMESTAMP,
            valid_to        TIMESTAMP,
            is_current      BOOLEAN
        )
        USING iceberg
        PARTITIONED BY (days(valid_from))
    """)


def merge_dimension(spark, table_name: str, config: dict, local_dir: str):
    # The core SCD2 logic for ONE dimension, driven entirely by `config`
    # (which table, which key, which attribute columns to compare).
    # Steps: (1) load + dedupe the incoming batch by PK, (2) figure out
    # which existing customers/products actually changed (materialized
    # into a Python list, not left as a lazy view), (3) MERGE: close
    # changed current rows + insert brand-new keys, (4) separately insert
    # the new "current" row for whichever keys were closed in step 3.
    iceberg_table = f"glue_catalog.{GLUE_DATABASE}.{config['iceberg_table']}"
    pk = config["pk"]
    attr_columns = list(config["attribute_columns"].keys())
    watermark_column = config["watermark_column"]

    source_df = spark.read.json(f"file://{local_dir}")

    # Spark's JSON reader has no native timestamp type -- it infers
    # STRING for date/time fields, since JSON itself has no datetime
    # type. Iceberg's write path validates column types strictly (no
    # implicit STRING -> TIMESTAMP cast allowed), so we cast explicitly
    # here, BEFORE it's used for dedup ordering or written as
    # valid_from/valid_to.
    source_df = source_df.withColumn(watermark_column, F.to_timestamp(F.col(watermark_column)))

    # extract.py's JSON serialization uses `default=str` for anything
    # json.dumps can't natively handle -- this catches datetimes AND
    # Decimal values alike, so DECIMAL-typed attribute columns (e.g.
    # products.price) also arrive as STRING and need an explicit cast,
    # same reasoning as the timestamp cast above.
    for col_name, col_type in config["attribute_columns"].items():
        if col_type.startswith("DECIMAL"):
            source_df = source_df.withColumn(col_name, F.col(col_name).cast(col_type))

    dedup_window = Window.partitionBy(pk).orderBy(F.col(watermark_column).desc())
    source_df = (
        source_df
        .withColumn("_row_num", F.row_number().over(dedup_window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )
    source_df.createOrReplaceTempView("source_batch")
    print(f"  {table_name}: incoming batch (deduped) = {source_df.count()} rows")

    changed_condition = " OR ".join(f"target.{c} != source.{c}" for c in attr_columns)

    changed_rows = spark.sql(f"""
        SELECT DISTINCT source.{pk}
        FROM source_batch source
        JOIN {iceberg_table} target
          ON target.{pk} = source.{pk} AND target.is_current = true
        WHERE {changed_condition}
    """).collect()
    changed_ids = [row[pk] for row in changed_rows]
    print(f"  {table_name}: detected {len(changed_ids)} row(s) with a changed attribute")

    insert_cols_sql = ", ".join([pk] + attr_columns + ["valid_from", "valid_to", "is_current"])
    insert_vals_sql = ", ".join([f"source.{pk}"] + [f"source.{c}" for c in attr_columns] + [f"source.{watermark_column}", "NULL", "true"])

    spark.sql(f"""
        MERGE INTO {iceberg_table} AS target
        USING source_batch AS source
        ON target.{pk} = source.{pk} AND target.is_current = true
        WHEN MATCHED AND ({changed_condition}) THEN UPDATE SET
            target.is_current = false,
            target.valid_to = source.{watermark_column}
        WHEN NOT MATCHED THEN INSERT ({insert_cols_sql}) VALUES ({insert_vals_sql})
    """)

    if changed_ids:
        spark.createDataFrame([(cid,) for cid in changed_ids], [pk]).createOrReplaceTempView("changed_ids")

        new_version_cols_sql = ", ".join(
            [f"source.{pk}"] + [f"source.{c}" for c in attr_columns]
            + [f"source.{watermark_column} AS valid_from", "CAST(NULL AS TIMESTAMP) AS valid_to", "true AS is_current"]
        )
        spark.sql(f"""
            INSERT INTO {iceberg_table}
            SELECT {new_version_cols_sql}
            FROM source_batch source
            JOIN changed_ids c ON c.{pk} = source.{pk}
        """)
    else:
        print(f"  {table_name}: no changed rows -- nothing to insert for new current versions")

    total = spark.sql(f"SELECT COUNT(*) AS c FROM {iceberg_table}").collect()[0]["c"]
    print(f"  {table_name}: {config['iceberg_table']} now has {total} total rows (all versions)")


def main():
    # Entry point: for each configured dimension (customers, products),
    # pull all its raw S3 files and run the SCD2 merge. Skips a
    # dimension cleanly if it has no raw data yet, rather than erroring.
    s3_client = boto3.client("s3")
    spark = build_spark_session()

    for table_name, config in DIMENSIONS.items():
        raw_keys = list_all_raw_keys(s3_client, table_name)
        if not raw_keys:
            print(f"  {table_name}: no raw data in S3 yet, skipping")
            continue

        local_dir = download_all_to_local_dir(s3_client, raw_keys)
        ensure_table_exists(spark, config)
        merge_dimension(spark, table_name, config, local_dir)

    spark.stop()
    print("Done.")


if __name__ == "__main__":
    main()
