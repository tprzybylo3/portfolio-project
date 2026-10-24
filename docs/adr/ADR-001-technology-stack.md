# ADR-001: Technology stack — AWS + Apache Iceberg + Snowflake + dbt + Airflow

## Status
Accepted. The decision was made on 2026-09-17 in the original project plan; it is recorded
here as an ADR retrospectively (2026-10-24), together with what was actually implemented.

## Context

The goal is a portfolio project that demonstrates modern data engineering end to end, and
doubles as interview preparation. The author's daily stack at work is Azure (Data Factory,
Synapse, ADLS Gen2), Databricks, PySpark and Power BI — another Azure/Databricks project
would add little new evidence of skill.

Business scenario: an e-commerce analytics platform that combines transactional data (orders,
customers, products, payments in PostgreSQL) with engineering activity from the GitHub API.
The GitHub source is deliberately simpler — a demonstration of API ingestion patterns
(pagination, rate limits, multithreading), not a forced joint business model.

Constraints: one person, a private budget (AWS free tier, a 30-day Snowflake trial with
$400 of credit, Databricks Free Edition), and the risk of "two months of configuring
infrastructure instead of learning data engineering" — a working pipeline should exist
early and grow from there.

## Decision

| Layer | Choice | Why |
|---|---|---|
| Sources | PostgreSQL (OLTP, Docker) + GitHub REST API | realistic incremental extraction (`updated_at`, triggers) + API pagination and limits |
| Cloud | AWS (S3, IAM, Glue) | broadens the skill set beyond Azure; free tier |
| Raw layer | S3, append-only | replay and audit, idempotency in `MERGE` (ADR-002) |
| Table format | Apache Iceberg | open and multi-engine: Spark writes, Athena and Snowflake read the same tables; Snowflake also writes Iceberg natively |
| Catalog | AWS Glue Data Catalog via the Iceberg REST endpoint | Snowflake's recommended integration (ADR-004) |
| Processing | PySpark (Docker locally) | `MERGE`, SCD Type 2, idempotent upserts with the industry-standard engine |
| Warehouse | Snowflake (Enterprise trial) | native Iceberg support, RBAC, dynamic masking, row access policies |
| Transformation | dbt Core | SQL models with tests, lineage and documentation; Kimball marts |
| Orchestration | Apache Airflow (Docker, LocalExecutor) | the most widespread open-source orchestrator; data intervals, retries, backfill |
| Data quality | custom validation first (`validate.py`), then dbt tests and `dbt-expectations` | a light gate early; framework comparison (Great Expectations, Soda) later |
| CI/CD, IaC | GitHub Actions, Terraform | planned phases 7–8 |
| Tooling | Docker Compose, uv, Git/GitHub (squash merge, conventional commits) | reproducible local environment |

**Deliberately out of scope:** Kafka/streaming, managed ingestion (Fivetran/Airbyte/dlt —
the point is to show the mechanics; a dlt comparison is planned), managed Airflow (MWAA) and
EC2 (cost; local Docker instead), certifications, FinOps as a separate thread.

**Phase order:** 0 architecture and repo → 1 sources and ingestion → 2 basic data quality →
3 lakehouse (Spark + Iceberg) → 4 warehouse (Snowflake) → 5 dbt → 6 orchestration
(Airflow) → 7 CI/CD → 8 Terraform → 9 observability. The Snowflake trial was started only
once phases 0–3 were ready, so that its 30 days were spent on phases 4–6.

**Core vs Advanced:** the core is a complete, working pipeline; advanced items (Great
Expectations in full, Elementary/observability, extended governance, a Databricks/Delta
comparison in `experiments/databricks/` within the same repo) are added on top of it, never
blocking it.

## Alternatives considered

- **Azure + Databricks + Delta Lake (the work stack).** Rejected as the main stack: already
  proven at work, so little new signal. Databricks is kept for a comparison experiment
  (Delta vs Iceberg vs Delta UniForm).
- **Snowflake only (native tables, `COPY INTO`, no Spark or Iceberg).** Rejected: it hides
  the lakehouse and multi-engine part. It would be the right choice in a pure Snowflake shop
  (see ADR-002).
- **Managed services everywhere (MWAA, AWS Glue jobs, Fivetran).** Rejected for cost and
  because they hide exactly the mechanics the project is meant to show.
- **Redshift or BigQuery as the warehouse.** Not chosen: Snowflake's native Iceberg
  read/write and its governance features fit the multi-engine goal; Athena serves as a
  lightweight second reader of the same tables.

## Consequences

**Positive:**
- A broad, transferable skill set: Azure/Databricks at work plus AWS/Iceberg/Snowflake here.
- A real multi-engine story: Spark writes, Athena and Snowflake read the same Iceberg tables.
- The whole pipeline runs locally in Docker, apart from the cloud storage and the warehouse.

**Negative / trade-offs:**
- The Snowflake trial is time-boxed: after 2026-10-27 the warehouse and dbt parts are
  documented with screenshots and logs rather than shown live.
- More moving parts than a single-platform stack (catalog integration, external volumes,
  IAM roles per consumer).
- Self-hosted Airflow on one machine is not production-like; a managed or cloud deployment is
  left for later phases.

## Related

- ADR-002 (raw layer and `MERGE`), ADR-004 (Snowflake ↔ Iceberg catalog integration),
  ADR-006 (extraction window)
- `README.md` — architecture, evidence and the design questions
