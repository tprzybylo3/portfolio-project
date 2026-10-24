# ADR-004: Snowflake–Iceberg Catalog Integration Strategy — REST + External Volume Credentials vs Vended Credentials

## Status
Accepted

## Context

Phase 3 is complete: 6 Iceberg tables (`dim_customer`, `dim_product`, `fact_orders`,
`fact_order_items`, `fact_payments`, `fact_order_status_history`) exist and hold real
data, managed by the Glue Data Catalog (database `portfolio_ecommerce`) in S3
(`tp-portfolio-ecommerce-raw`). Phase 4 connects Snowflake to the same tables (native
read, no data copy) as the warehousing layer for dbt.

Snowflake offers two ways to integrate with AWS Glue as the Iceberg catalog:

- **Classic integration (`CATALOG_SOURCE = GLUE`)** — the older mechanism, direct calls
  to the Glue API (`GetTable`/`GetTables`).
- **REST (`CATALOG_SOURCE = ICEBERG_REST`, `CATALOG_API_TYPE = AWS_GLUE`)** — the newer
  mechanism through Glue's Iceberg REST endpoint (SigV4), currently officially recommended
  by Snowflake ("we recommend that you instead create a catalog integration for the AWS
  Glue Iceberg REST endpoint").

The REST path adds a second decision — `ACCESS_DELEGATION_MODE`:

- `VENDED_CREDENTIALS` — Snowflake gets access to S3 indirectly, through credentials
  issued by the catalog; for AWS Glue this in practice involves AWS Lake Formation as the
  access management layer.
- `EXTERNAL_VOLUME_CREDENTIALS` (default) — Snowflake connects to S3 directly through the
  IAM role specified in the external volume, conceptually the same way Spark/Iceberg
  already connect to S3 in this project.

The project does not have (and did not plan) Lake Formation in its infrastructure — the
current access model is a simple IAM user/role with a policy on S3 + Glue.

## Decision

1. **A REST catalog integration** (`CATALOG_SOURCE = ICEBERG_REST`,
   `CATALOG_API_TYPE = AWS_GLUE`, `REST_AUTHENTICATION TYPE = SIGV4`) — aligned with
   Snowflake's current recommendation, instead of the classic `GLUE` integration, which
   Snowflake already marks as being phased out.

2. **`ACCESS_DELEGATION_MODE = EXTERNAL_VOLUME_CREDENTIALS`**, not
   `VENDED_CREDENTIALS`. We avoid introducing Lake Formation as a new, parallel access
   management layer next to the existing, working IAM-role-per-service model. Fewer moving
   parts, without losing any functionality relevant to this project.

3. **A catalog-linked database** (`CREATE DATABASE ... LINKED_CATALOG = (...)`) instead of
   declaring `CREATE ICEBERG TABLE` manually per table — automatic discovery of the
   existing namespaces/tables from the Glue Catalog, consistent with the fact that the
   tables already exist (Phase 3) and are not created from scratch by Snowflake.

4. A new IAM role (not an extension of the existing `portfolio-pipeline` user) with a trust
   policy allowing Snowflake to `AssumeRole` it (external ID) — a separate identity per
   catalog consumer, following the least-privilege principle already applied in the
   project (an IAM user without console access).

## Alternatives considered

**Alternative 1 — The classic `CATALOG_SOURCE = GLUE` integration.**
Rejected because:
- Snowflake explicitly recommends REST as its successor and points to additional
  capabilities (including vended credentials) not available in the older path
- there is no business or learning reason to learn a mechanism marked as legacy when REST
  is just as available from the start

**Alternative 2 — `ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS` with Lake Formation.**
Rejected for this stage of the project because:
- it would require introducing a whole new service (Lake Formation) only to change how
  credentials are passed, without a real functional benefit at the current scale (one
  catalog consumer — Snowflake)
- it increases the surface to learn at the same time as the rest of Phase 4 (RBAC,
  masking, RLS), diluting the priorities of that phase
It would be a good choice if the project eventually had many catalog consumers (Snowflake +
Athena + Spark + another engine) requiring a consistent, centralised table/column-level
access policy in one place — exactly what Lake Formation is designed for.

## Consequences

**Positive:**
- Aligned with Snowflake's current direction (REST), without learning a path that is being
  phased out
- An access model mentally consistent with the rest of the project (IAM role + policy),
  without a new, parallel authorisation layer
- The catalog-linked database removes the need to repeat the definitions of 6 tables by
  hand

**Negative / trade-offs:**
- No Lake Formation demonstration in the project — a conscious gap, to be mentioned in an
  interview as a "next step", not as something overlooked
- The external volume is an additional Snowflake object to maintain (next to the catalog
  integration), which `VENDED_CREDENTIALS` would not require

## Related

- ADR-002 (Bronze ingestion strategy) — the same Glue Data Catalog and S3 bucket as the
  reference point for the Iceberg layer
- `PROGRESS_LOG.md`, section 12 (Phase 4 — start)
