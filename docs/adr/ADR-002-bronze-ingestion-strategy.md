# ADR-002: Bronze/Landing Ingestion Strategy — Append-only S3 + Iceberg MERGE vs Native Snowflake Bronze

## Status
Proposed (2026-09-17) → Accepted, implemented (update of 2026-10-24 at the end of this document)

## Context

Project pipeline: Postgres (incremental extract) + GitHub API (pagination) → S3 (raw/landing)
→ Great Expectations (validation) → PySpark → Apache Iceberg (S3 + Glue Data Catalog)
→ Snowflake (reads Iceberg natively) → dbt (staging → marts, Kimball star schema).

The key question: how to handle backfill/reprocessing of a historical date range (e.g. a
source error that requires re-fetching `2026-08-10` to `2026-08-12`) without losing
idempotency, without unbounded storage growth, and while keeping auditability
("what exactly did the source deliver in a given run").

The industry (discussions on idempotency patterns for ingestion, Databricks Medallion vs
Snowflake/Redshift) shows two competing patterns for the Bronze layer:

- **Pattern A — Purge-and-reload per partition**: on backfill you clear the S3 prefix
  and run a `DELETE` on Bronze before the next `COPY INTO`. Bronze holds exactly
  one snapshot per day.
- **Pattern B — Pure append-only + downstream dedup**: nothing is ever deleted from S3
  or from Bronze; every run (backfills included) adds new files/rows with a unique
  `run_id`. Deduplication via `QUALIFY ROW_NUMBER() OVER (PARTITION BY entity_id
  ORDER BY updated_at DESC, loaded_at DESC) = 1` in the Silver layer.

In addition, this project deliberately uses Apache Iceberg (not native Snowflake tables
with a VARIANT column), which changes the available options — Iceberg has built-in
time travel/snapshots at the table-format level, which a native Snowflake Bronze
(in the approach from the industry discussion) does not have and has to simulate
manually with append-only + `run_id`.

## Decision

1. **S3 raw/landing is always append-only and immutable.** Files are partitioned by
   ingestion date + `run_id` (not by the logical date of the data). Files in S3 are
   never overwritten or deleted — this is the "insurance policy" for replay.
   Retention: long (years in a real system; a week or a month of simulation is enough
   for this portfolio project), cheap (S3 Standard → lifecycle to Glacier after X days).

2. **No intermediate native Bronze table in Snowflake.** Instead of
   S3 → `COPY INTO` a native Bronze (VARIANT) → dedup in Silver, PySpark reads the raw
   files directly from S3 and runs an idempotent `MERGE INTO` on the Iceberg table,
   keyed on the natural business key + watermark (`updated_at`/CDC timestamp).

3. **History and auditability are provided by two different mechanisms at two
   levels:**
   - "What came from the source" → S3 raw (append-only, all runs, all backfills,
     nothing is lost)
   - "What the business data looked like over time" → Iceberg snapshots
     (audit/rollback of the whole table) + dbt snapshot / SCD Type 2 in the marts
     (history per business key) — not by manually keeping duplicates in the Bronze
     layer.

4. **Great Expectations validates at the extraction stage** (before data even becomes
   a candidate for `MERGE`), not after landing in a Bronze layer — fail-fast/quarantine
   before, not after.

5. **Iceberg snapshot retention is set shorter than S3 raw retention**
   (e.g. 30–90 days of snapshot history on the Iceberg side vs months/years on S3),
   because the actual business history lives in SCD Type 2 in the marts anyway —
   Iceberg snapshots mainly serve short-term rollback/debugging, not long-term audit.

## Alternatives considered

**Alternative 1 — Native Bronze in Snowflake (VARIANT + `COPY INTO`), pure
append-only + `QUALIFY ROW_NUMBER()` dedup in Silver.**
Rejected for this project because:
- it adds an extra "hop" into Snowflake's proprietary format instead of showcasing
  multi-engine Iceberg, which is the goal of this portfolio project
- it loses the natural time travel that Iceberg gives "for free" from the table format
It would still be a good choice in a company that is a pure Snowflake shop, with no
Spark or other engines in play — there the overhead of Iceberg (Glue Catalog, external
volume) would be unnecessary complexity.

**Alternative 2 — Purge-and-reload per partition (DELETE + reload).**
Rejected because:
- it is not atomic — the failure window between `DELETE` and the reload leaves Bronze
  half-loaded if the task dies in between
- it destroys the audit trail of what a given source run actually delivered
- it is inconsistent with the rest of the project, where idempotency is achieved with
  `MERGE`, not `DELETE+INSERT`

## Consequences

**Positive:**
- Full idempotency — every Airflow/PySpark task can be safely retried,
  `MERGE INTO` will not duplicate data
- Audit trail kept cheaply at the S3 level, without inflating warehouse compute/
  storage cost (no thousands of duplicated rows in Snowflake)
- Iceberg time travel provides rollback/audit "for free", without manually managing
  duplicates as in the pure Snowflake+VARIANT approach
- Clear separation of responsibilities: S3 = "what arrived", Iceberg snapshots =
  "short-term table audit", SCD2 = "long-term business history"

**Negative / trade-offs:**
- Requires two independent retention policies to be configured correctly
  (S3 lifecycle + Iceberg snapshot expiration) — a mistake in either one either drives
  costs up (kept too long) or destroys auditability (kept too short)
- More operational moving parts than "just COPY INTO a native table" — Glue Data
  Catalog, external volume, maintaining the PySpark job
- If the project ended up in an organisation that is a pure Snowflake shop without
  Spark, this pattern would need to be re-evaluated — native Snowflake tables would be
  simpler for them

## Related

- SCD Type 2 on Iceberg — business history implemented as SCD2, not as duplicates in
  Bronze (author's study notes, section 5.3; not part of the repository)
- Parquet vs Delta vs Iceberg — the multi-engine ecosystem as the reason for choosing
  Iceberg in this project (author's study notes, section 14.1)

## Update 2026-10-24 — state after implementation (phases 3–6)

The text above is the decision of 2026-09-17, left unchanged. The actual state:

- **Implemented as decided:** S3 raw is append-only, every run writes its own files
  with a `run_id`, nothing is overwritten or deleted; no native Bronze in Snowflake;
  PySpark reads the raw files from S3 and runs an idempotent `MERGE` into Iceberg (Glue
  Data Catalog), and Snowflake reads the Iceberg tables through a catalog-linked
  database (ADR-004). Idempotency confirmed in practice: the same window processed
  twice with no change in the tables (README, question 1).
- **Changed compared to the decision:**
  - Validation: a custom `validate.py` with quarantine and a hard gate instead of
    Great Expectations (GE deferred; a comparison of data-quality tools is planned
    after phase 6). The "fail fast before `MERGE`" principle is unchanged.
  - SCD Type 2 is done by Spark `MERGE` on the Iceberg tables (`customer`,
    `product`), not by dbt snapshots in the marts; dbt reads the already-historized
    tables (point-in-time join in `int_customer_versions`). The split "S3 = what
    arrived, business history = SCD2" is unchanged; only the tool changed.
  - Extraction window: the Airflow data interval with a lookback instead of watermark
    state (ADR-006).
- **Not done yet:** retention policies (S3 lifecycle, Iceberg snapshot expiration) —
  point 5 of the decision waits for Terraform.
- **Known limitation found during the backfill (2026-10-23):** the `MERGE` steps read
  the whole raw layer and deduplicate it instead of the current run's files. The result
  is correct, but the cost grows every day; fix: `MERGE` only over the files of its own
  `run_id`.
