# Architecture Decision Records

One file per decision, in the format: context → decision → alternatives considered →
consequences. Decisions are never rewritten: a changed state is added as a dated
update section in the same ADR, or as a new ADR that supersedes the old one.

| ADR | Decision | Status |
|---|---|---|
| [ADR-001](ADR-001-technology-stack.md) | Technology stack: AWS + Apache Iceberg + Snowflake + dbt + Airflow, and what was deliberately left out | Accepted (recorded retrospectively 2026-10-24) |
| [ADR-002](ADR-002-bronze-ingestion-strategy.md) | Raw layer: append-only S3 + `MERGE` into Iceberg instead of a native Bronze layer in Snowflake | Accepted, implemented (update 2026-10-24) |
| [ADR-003](ADR-003-ai-assisted-engineering-workflow.md) | AI-assisted engineering workflow: Claude Code, Codex review, hooks, failure log | Accepted |
| [ADR-004](ADR-004-snowflake-iceberg-catalog-integration.md) | Snowflake ↔ Iceberg: Glue REST catalog integration with external volume credentials instead of vended credentials | Accepted |
| [ADR-005](ADR-005-layer-naming-convention.md) | Layer naming: `dim_`/`fct_` only for marts; Iceberg tables renamed | Accepted, implemented (update 2026-10-22) |
| [ADR-006](ADR-006-extract-window-strategy.md) | Extraction window: Airflow data interval with a lookback instead of watermark state | Accepted, implemented (phase 6) |
