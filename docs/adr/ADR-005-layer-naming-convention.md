# ADR-005: Layer naming — `dim_`/`fct_` for marts only; renaming the Iceberg tables

## Status
Accepted. Implemented and verified: rename in Glue (row counts and `metadata_location`
identical before/after), code updated, masking/RLS re-applied in Snowflake, and `dbt
run`/`dbt test` (confirmed by the user, output not preserved).

## Context

Phase 3 named the 6 Iceberg tables in Kimball style from the start: `dim_customer`,
`dim_product` (full SCD Type 2), `fact_orders`, `fact_order_items`, `fact_payments`,
`fact_order_status_history`. Physically they sit before the warehouse and before dbt,
i.e. in the Bronze/Silver layer. In company practice and in dbt convention, the `dim_`/`fct_`
prefixes denote final marts ready for Power BI. Two problems followed:

1. The name suggests a consumption layer that the object does not represent yet.
2. The planned dbt marts would collide by name with the source tables (two different
   `dim_customer` objects in two databases).

The decision was made before the marts were built, while the cost of change was still
small. ChatGPT was consulted for a second opinion (a critical assessment of the options),
and the change plan went through Plan Mode in Claude Code.

## Decision

1. **Option A: rename the Iceberg tables** to `customer`, `product`, `orders`,
   `order_items`, `payments`, `order_status_history`. The `dim_`/`fct_` prefixes are
   reserved exclusively for dbt marts.
2. **Rule for Gold:** an object in Gold is created only if it adds logic (joins,
   aggregations, derived attributes). A Gold `dim_customer` that is a copy of
   `stg_customers` is not created. Marts answer business questions
   (`fct_customer_revenue`, `fct_product_sales`), and a Gold dimension makes sense only when
   enriched (e.g. `first_order_date`, lifetime revenue).
3. Only table names change. Column names, the Glue database name `portfolio_ecommerce` and
   the `stg_*` model names stay. (The S3 paths were originally meant to stay as well;
   changed on 2026-10-07, see "Update" below.)

## Alternatives considered

**B: keep the names, explain them in documentation.** Rejected: documentation is a worse
interface than a good name, and the exception would have to be explained to every new
person. The cost of a misleading name is paid at every onboarding and code review; the
cost of a rename is paid once.

**C: separate namespaces per layer** (e.g. a Glue database `silver`, `dim_`/`fct_` in a
separate Gold schema). Rejected for now, not in principle: the Glue database name is
written into the IAM policy (resource ARNs), the catalog integration's
`CATALOG_NAMESPACE`, the catalog-linked database, the Spark configuration and dbt's
`source()`, so it is a contract change in five places instead of renaming six tables. Layer
separation is provided anyway by the database split on the Snowflake side
(`portfolio_ecommerce_db` vs `portfolio_dbt`). To be reconsidered at a larger project
scale.

## How the rename was done (migration pattern)

Glue has no native rename; Iceberg's `GlueCatalog` implements it as creating a new entry
and deleting the old one, without touching the files. Through Spark
(`ALTER TABLE ... RENAME TO`, iceberg-spark-runtime 1.9.1): a snapshot before (row count,
`metadata_location`), a pilot on a table without policies (`dim_product`), the remaining
tables one by one with a check after each, a snapshot after and a 6/6 comparison, only then
the code update, and a pause before exposing anything in Snowflake. `DROP ... PURGE` was
forbidden. The fallback (`register_table` + deleting the entry without purge) was not
needed.

## Consequences

**Positive:**
- The name tells the truth about the layer; no collisions with marts.
- Marts are designed from business questions, without duplicating the dimensional model.
- A documented, repeatable pattern for safely migrating Iceberg tables.

**Negative / to remember:**
- **Governance is attached to the catalog object, not to the data.** For Snowflake, a
  rename in Glue means "the old table disappeared, a new one appeared": masking/RLS
  policies (and per-table grants) have to be re-applied manually and tested.
- ~~The S3 data directories kept the old names (`.../dim_customer/`). A consciously
  accepted cost.~~ Outdated since 2026-10-07, see "Update".
- Older sections of `PROGRESS_LOG.md`, ADRs and general examples in the study notes keep
  the old names as a record of the state at that time.

## Update 2026-10-07: relocating the S3 paths

**Change of decision:** the S3 paths were aligned with the table names
(`.../portfolio_ecommerce.db/customer/` instead of `.../dim_customer/` etc.). The mismatch
between the name in the catalog and the directory in S3 turned out to be a real cost when
navigating the bucket, and doing it before Phase 6 (Airflow) means the DAG works on the
final layout from the start.

**Method:** `rewrite_table_path` (rewriting the paths in the metadata) + copying the files
on the S3 side + `register_table` in a separate Glue database
`portfolio_ecommerce_relocation` (invisible to Snowflake, because the catalog integration
role only has permissions on `portfolio_ecommerce`) + a swap via two renames. CTAS was
rejected: it lost the snapshot history (including the rollback point from §23) and required
recreating the partition spec by hand. Pilot on `product`, then one by one, `customer`
last.

**Result:** 6/6 tables — rows, content hash, snapshots and time travel identical; only the
location changed. The old directories were deleted after verification. Script and evidence:
`scripts/repairs/2026-10-07_relocate_table_paths/`.

**Finding (complements the governance consequence):** a relocation that keeps the name +
`ALTER ICEBERG TABLE ... REFRESH` in Snowflake **keeps** the policies and grants, because
for Snowflake it is the same object with a new metadata pointer. Only a name change loses
them.

## Update 2026-10-22: a hidden side effect of the relocation in Snowflake

**What happened:** the relocation of 10-07 left automatic refresh stopped in Snowflake for
5 of the 6 tables of the catalog-linked database (`executionState:
GENERALIZED_PIPE_STOPPED` since 10-07 11:38; only `order_status_history` worked, where
`MERGE` only appends). For two weeks Snowflake served the pre-relocation state and did not
move to newer metadata versions (Glue: `00005`, Snowflake: `00003`). The refresh error:
"invalid file … was found in the Iceberg metadata snapshot", and queries after the old
directories were deleted: "parquet file … fact_orders/… was inaccessible".

**Mechanism (most likely):** `rewrite_table_path` changes file paths but keeps snapshot
IDs. Snowflake refreshes a table incrementally by snapshot and remembers which files each
snapshot added; a file with a new path in a "known" snapshot was treated as invalid and the
refresh stopped. It worked as long as the old files existed; after they were deleted from
S3 (the cleanup in §25) reads started returning duplicates or errors. The verification in
§25 checked Glue and Spark, not the state of Snowflake.

**Detection:** dbt `unique` tests on staging in the first DAG run with the dbt task
(10-22); dbt skipped the dependent models (`fct_orders` was not rebuilt).

**Fix:** a manual `ALTER ICEBERG TABLE ... REFRESH` returned the same error. The
catalog-linked database was recreated with the same statement (from `GET_DDL`):
`DROP DATABASE` + `CREATE DATABASE ... LINKED_CATALOG` → Snowflake discovered the 6 tables
from scratch on the current metadata; then the policies on `customer` (before grants), then
the grants for `portfolio_analyst`. Safe for the data: the Snowflake role has read-only
access in Glue, and the external volume has `ALLOW_WRITES = FALSE`. Verification: counts
51/30/200/502/160/493 (matching Spark, `orders` 200/200), metadata `00005`, refresh
`RUNNING`, the analyst sees only DE and a masked email, `dbt build` PASS=51. Script:
`snowflake/2026-10-22_recreate_catalog_linked_db.sql`.

**Procedure change (complements the "safe migration pattern"):** after physically moving an
Iceberg table, check **every engine separately**, not just the catalog: in Snowflake,
`SYSTEM$AUTO_REFRESH_STATUS` (`RUNNING`) and `SYSTEM$GET_ICEBERG_TABLE_INFORMATION` (the
same metadata version as in Glue), **before** deleting the old files. When relocating with
`rewrite_table_path`, assume the catalog-linked database will have to be recreated.

## Related

- ADR-002 (the same Glue database and S3 bucket), ADR-004 (catalog integration and the IAM
  policy with the wildcard `table/portfolio_ecommerce/*`, which a rename does not break)
- `PROGRESS_LOG.md`, sections 21 (rename) and 25 (path relocation)
