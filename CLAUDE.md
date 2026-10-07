# CLAUDE.md — project context for Claude Code

This file is read automatically at the start of every session. Update it when
conventions change — don't duplicate decision history here (that lives in
`PROGRESS_LOG.md` and `ADR-*.md`), only what Claude Code MUST know to write
code consistent with the rest of the project.

## What this project is

Data engineering portfolio: Postgres+GitHub API → S3 (raw) → custom
validation (`validate.py` with quarantine; Great Expectations deliberately
deferred) → PySpark + Apache Iceberg (Glue Data Catalog) →
Snowflake (reads Iceberg natively, RBAC/masking/RLS) → **dbt (this phase)** →
Airflow (next phase). Full architecture and rationale:
`project-summary-for-consultation.md`, `ADR-002`, `ADR-004`, `ADR-005`.
Step-by-step build history with Q&A: `PROGRESS_LOG.md`. General theory with
examples (not specific to this project): `DE_Interview_Cheatsheet-2.docx`.

## State at the start of this phase

- 6 Iceberg tables exist and hold data, managed by Spark: `customer` and
  `product` (both full SCD Type 2), `orders`, `order_items`, `payments`,
  `order_status_history`. (They were renamed from `dim_*`/`fact_*` — see
  decision 5. Old S3 directories keep the old names; that is expected.)
- Available in Snowflake through a catalog-linked database
  `portfolio_ecommerce_db.portfolio_ecommerce` (REST/Glue catalog
  integration, **read-only** external volume).
- RBAC in place: roles `portfolio_analyst`/`portfolio_engineer`
  (inheritance).
- Governance in place: masking (`email`) and a row access policy (`country`)
  on `customer`, policy objects live in `portfolio_governance.policies`
  (a separate, standard database — a catalog-linked database cannot host
  policies).
- dbt staging exists: 6 `stg_*` views in `portfolio_dbt.dbt_dev_staging`,
  17 tests.
- `customer` SCD2 dates repaired to event time (PROGRESS_LOG §23). Seed data
  still has orders dated before the customer's first version (independent
  random dates in `seed.py`) — expected, handle it in dbt, don't "fix" the source.

## Decisions binding for this phase (don't renegotiate without asking the user)

1. **SCD Type 2 stays in the Iceberg/Spark layer.** dbt does NOT run
   `dbt snapshot` on `customer`/`product` — staging models read the
   already-historized SCD2 data directly (`WHERE is_current = true` for the
   current state, point-in-time join on `valid_from`/`valid_to` when a fact
   needs a historical match). Reason: avoids duplicated history and preserves
   the Iceberg multi-engine value (see cheat sheet 5.3).
2. **No separate geography dimension.** `country` stays as a degenerate
   attribute on `customer` — no hierarchy that would justify its own
   dimension.
3. **Marts (Gold) as Snowflake-managed Iceberg tables** (`CATALOG =
   'SNOWFLAKE'`), NOT native Snowflake tables — a deliberate choice against
   vendor lock-in, while keeping full Snowflake performance/governance.
   Requires a SECOND external volume with `ALLOW_WRITES = TRUE` (separate
   from the read-only volume from Phase 4). **Exists:** `portfolio_gold_ext_vol`
   (`s3://tp-portfolio-ecommerce-raw/gold/`), USAGE granted to
   `portfolio_engineer`, verified with `SYSTEM$VERIFY_EXTERNAL_VOLUME`.
4. **Governance policies (masking/RLS) on marts**, if needed, go into
   `portfolio_governance.policies` — same pattern as Phase 4. Never create
   governance objects inside a catalog-linked database.
5. **Naming by layer (ADR-005).** The `dim_`/`fct_` prefixes are reserved for
   final marts built by dbt. Tables before dbt (Iceberg) have no such prefix.
   **A Gold object is created only if it adds logic** (joins, aggregations,
   derived attributes). Do NOT create a Gold `dim_customer` that merely
   copies `stg_customers`. Marts answer business questions
   (`fct_customer_revenue`, `fct_product_sales`, `fct_order_funnel`); a Gold
   dimension is justified only when enriched (e.g. first_order_date, lifetime
   revenue, segment).
6. **Renaming or recreating a table in Glue drops its Snowflake policies.**
   Governance is attached to the catalog object, not to the data. If a task
   involves that, stop before `dbt run` so the user can re-attach masking/RLS
   and grants manually, then test both roles.

## Code conventions (from prior work on this project)

- **Intermediate DataFrame/CTE names are descriptive, never numbered**
  (`_1`, `_2`) — the name should reflect the transformation/business
  meaning of the step, not its order.
- Config-driven where logic is identical across many tables (pattern from
  `merge_dimensions.py` — one file, parameterized by a dict/config),
  SEPARATE files/models where the logic genuinely differs algorithmically
  (pattern: `merge_facts.py` vs `merge_dimensions.py`) — don't force-unify
  different logic just for uniformity.
- SQL: uppercase keywords, snake_case names.
- One dbt model = one `.sql` file + an entry in `schema.yml` (description +
  tests) next to it.
- Verify assumptions about types empirically (e.g. `DESCRIBE TABLE`), don't
  rely on documentation claims. Example: source timestamps are
  `TIMESTAMP_LTZ(6)`, hence `CONVERT_TIMEZONE('UTC', col)::TIMESTAMP_NTZ`.

## dbt project structure

```
dbt_portfolio/
├── models/
│   ├── staging/                 # exists: stg_customers, stg_products,
│   │   ├── stg_*.sql            # stg_orders, stg_order_items, stg_payments,
│   │   └── schema.yml           # stg_order_status_history
│   └── marts/                   # to build (names are examples, driven by
│       ├── fct_customer_revenue.sql   # business questions, see decision 5)
│       ├── fct_product_sales.sql
│       ├── fct_order_funnel.sql
│       └── schema.yml           # tests: unique, not_null, relationships
├── dbt_project.yml
└── profiles.yml.example         # real profiles.yml lives in ~/.dbt (never in repo)
```

## Secrets — how dbt connects to Snowflake

**Never hardcode a password/key in `profiles.yml` or anywhere in the repo.**
Auth for the service account is **key-pair** (cheat sheet 14.4), not a
password. The real `profiles.yml` lives at `~/.dbt/profiles.yml` (outside the
repo) and reads environment variables (`DBT_SNOWFLAKE_*`). The agent's shell
does not have them: the user runs `dbt run`/`dbt test` and pastes the result.
The repo contains only `profiles.yml.example` with placeholders.

## Guardrails (treat as hard rules, not suggestions)

- Never generate or execute `DROP TABLE`, `DROP DATABASE`, `TRUNCATE`
  without explicit, one-time confirmation from the user in the current
  session. Never `DROP ... PURGE` on Iceberg tables.
- Never write secrets (passwords, private keys, tokens) to any file in the
  repo — including `.md`/`.yml` files used as examples.
- Do not create a `dbt snapshot` on `customer`/`product` (see decision 1) —
  if you think one is genuinely needed, ask, don't assume.
- After changing a model: `dbt run --select <model>` +
  `dbt test --select <model>` before moving on (run by the user when the
  session has no credentials).
- For operations on live data (rename, migration): snapshot state before and
  after (row counts, `metadata_location`), pilot on the lowest-risk object,
  one object at a time, stop on any mismatch.
- If you hit a real bug (yours, not the environment's) while implementing,
  flag it explicitly — it goes into `ai/failure-log/` per ADR-003.

## After finishing a task

Don't update `PROGRESS_LOG.md`/ADRs yourself — that's done by the project
layer (the conversation with Claude outside Claude Code) after reviewing
your diff. Leave a clear summary of changes at the end of the session.
