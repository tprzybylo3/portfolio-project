# 001: `rewrite_table_path` does not work in the Spark container without a Hadoop S3 FileSystem

**Date:** 2026-10-07
**Task:** relocating the 6 Iceberg tables to S3 paths matching their names (ADR-005)
**Agent:** Claude Code
**Impact:** none on the data. The pilot on `product` stopped before any change in Glue or in
the target directory. 4 rewritten metadata files were left in
`s3://tp-portfolio-ecommerce-raw/migration-staging/2026-10-07/product/`. Nothing references
them and they sit outside `warehouse/`.

## AI mistake

In the plan I recommended option (b): `rewrite_table_path` + `register_table`. I considered
it feasible because I only verified that the procedure is in the jar
(`RewriteTablePathProcedure` in `iceberg-spark-runtime-3.5_2.12-1.9.1`, parameters from
`javap`). I did not check whether it can be run in this container configuration, while the
plan promised "verify, don't assume".

## How it was detected

The first run of `relocate.py prepare product`, i.e. the planned pilot on the lowest-risk
table. The error:
`org.apache.hadoop.fs.UnsupportedFileSystemException: No FileSystem for scheme "s3"`
in `RewriteTablePathSparkAction.saveFileList`.

## Root cause

The Spark action writes the rewritten metadata through the table's `FileIO` (`S3FileIO`,
works), but it writes the list of files to copy as a Spark Dataset through the **Hadoop
FileSystem**. The whole pipeline (Glue + `S3FileIO` from `iceberg-aws-bundle`) never needed
Hadoop access to S3. That is why the image only has `hadoop-client` 3.3.4 from PySpark,
without `hadoop-aws`, and the `s3://` scheme has no FileSystem implementation. The presence
of a procedure in a jar says nothing about its runtime dependencies.

## Fix

In the Spark session in `relocate.py` (without changing the image):
`spark.jars.packages=org.apache.hadoop:hadoop-aws:3.3.4` (a version matching the Hadoop of
PySpark 3.5.5), `spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem` and the
regional endpoint. In addition, a unique staging directory per run
(`.../<table>/<run id>/`), so that leftovers of a failed run do not block the next one and
nothing has to be deleted along the way.

## Prevention

- When verifying a tool in a plan, distinguish "exists" from "works here". The cheapest
  proof is running it on a risk-free object before recommending the path. Here the pilot
  played that role, but the recommendation came earlier.
- For Spark actions with Iceberg, check whether they write results through the table's
  `FileIO` or through Spark/Hadoop. These are two different S3 access paths in the same
  container.
- The pattern that worked: a snapshot before, a pilot on a table without policies, steps
  writing only outside production objects until verified. Thanks to that, an environment
  error did not touch the data.

## Second appearance of the same mistake (same pilot)

After adding `hadoop-aws` the procedure passed, but the script failed with `NoSuchKey`.
`file_list_location` is not a single file but a Spark CSV output directory (`part-*.csv` +
`_SUCCESS`). Same root cause: I assumed how the action writes its results instead of
checking. Fix: `read_file_list` reads all `part-*.csv` files from that directory. The script
stopped before copying, so the target directory stayed empty and nothing was created in
Glue.

## Detection layer

Manual, through the planned pilot on the live environment. No hook, test or review caught
it. The pilot is part of the procedure (CLAUDE.md, guardrails), not a deterministic test.
