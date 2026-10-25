-- 2026-10-22: recreate the catalog-linked database after its auto-refresh had
-- been stopped since the 2026-10-07 relocation of the Iceberg tables
-- (rewrite_table_path kept snapshot IDs, changed file paths; Snowflake rejected
-- the files: "invalid file ... was found in the Iceberg metadata snapshot").
-- Context: ADR-005 (update 2026-10-22), PROGRESS_LOG §26, CLAUDE.md decision 6.
--
-- Safe for data: the Snowflake Glue role is read-only (Get* only) and the
-- external volume has ALLOW_WRITES = FALSE, so dropping the database cannot
-- touch Glue or S3. Run as ACCOUNTADMIN. Order matters: policies BEFORE grants.

-- 0. Inventory first (read-only): exact DDL, grants, policies
--    SELECT GET_DDL('DATABASE', 'PORTFOLIO_ECOMMERCE_DB');
--    SELECT grantee_name, privilege, granted_on, table_schema, name
--      FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES
--     WHERE deleted_on IS NULL
--       AND (table_catalog = 'PORTFOLIO_ECOMMERCE_DB' OR name = 'PORTFOLIO_ECOMMERCE_DB');
--    SHOW FUTURE GRANTS IN SCHEMA portfolio_ecommerce_db."portfolio_ecommerce";
--    SELECT * FROM TABLE(portfolio_ecommerce_db.INFORMATION_SCHEMA.POLICY_REFERENCES(
--      ref_entity_name => 'portfolio_ecommerce_db."portfolio_ecommerce"."customer"', ref_entity_domain => 'table'));

USE ROLE ACCOUNTADMIN;

-- 1. Recreate (same statement as GET_DDL returned)
DROP DATABASE portfolio_ecommerce_db;
CREATE DATABASE PORTFOLIO_ECOMMERCE_DB
  LINKED_CATALOG = (CATALOG = 'GLUE_REST_CATALOG_INT' NAMESPACE_MODE = IGNORE_NESTED_NAMESPACE SYNC_INTERVAL_SECONDS = 30)
  EXTERNAL_VOLUME = 'PORTFOLIO_ECOMMERCE_EXT_VOL';

-- 2. WAIT ~1 minute until the 6 tables are discovered:
--    SHOW ICEBERG TABLES IN DATABASE portfolio_ecommerce_db;

-- 3. Policies BEFORE grants (the analyst must never see customer unprotected)
ALTER ICEBERG TABLE portfolio_ecommerce_db."portfolio_ecommerce"."customer"
  MODIFY COLUMN "email" SET MASKING POLICY portfolio_governance.policies.email_mask;
-- first/last name: IDENTIFIER per SYSTEM$CLASSIFY (2026-10-25)
ALTER ICEBERG TABLE portfolio_ecommerce_db."portfolio_ecommerce"."customer"
  MODIFY COLUMN "first_name" SET MASKING POLICY portfolio_governance.policies.name_mask;
ALTER ICEBERG TABLE portfolio_ecommerce_db."portfolio_ecommerce"."customer"
  MODIFY COLUMN "last_name" SET MASKING POLICY portfolio_governance.policies.name_mask;
ALTER ICEBERG TABLE portfolio_ecommerce_db."portfolio_ecommerce"."customer"
  ADD ROW ACCESS POLICY portfolio_governance.policies.country_rls ON ("country");

-- 4. Grants (portfolio_engineer inherits portfolio_analyst)
GRANT USAGE ON DATABASE portfolio_ecommerce_db TO ROLE portfolio_analyst;
GRANT USAGE ON SCHEMA portfolio_ecommerce_db."portfolio_ecommerce" TO ROLE portfolio_analyst;
GRANT SELECT ON ALL ICEBERG TABLES IN SCHEMA portfolio_ecommerce_db."portfolio_ecommerce" TO ROLE portfolio_analyst;
GRANT SELECT ON FUTURE TABLES IN SCHEMA portfolio_ecommerce_db."portfolio_ecommerce" TO ROLE portfolio_analyst;

-- 5. Verify (results 2026-10-22: 51 / 30 / 200/200 / 502 / 160 / 493, metadata 00005, RUNNING;
--    as portfolio_analyst: 2 rows, both DE, email ***@example.org)
ALTER SESSION SET USE_CACHED_RESULT = FALSE;
SELECT
  (SELECT COUNT(*) FROM portfolio_ecommerce_db."portfolio_ecommerce"."customer")             AS customer,
  (SELECT COUNT(*) FROM portfolio_ecommerce_db."portfolio_ecommerce"."product")              AS product,
  (SELECT COUNT(*) || '/' || COUNT(DISTINCT "order_id") FROM portfolio_ecommerce_db."portfolio_ecommerce"."orders") AS orders,
  (SELECT COUNT(*) FROM portfolio_ecommerce_db."portfolio_ecommerce"."order_items")          AS order_items,
  (SELECT COUNT(*) FROM portfolio_ecommerce_db."portfolio_ecommerce"."payments")             AS payments,
  (SELECT COUNT(*) FROM portfolio_ecommerce_db."portfolio_ecommerce"."order_status_history") AS order_status_history,
  PARSE_JSON(SYSTEM$GET_ICEBERG_TABLE_INFORMATION('portfolio_ecommerce_db."portfolio_ecommerce"."orders"')):metadataLocation::STRING AS orders_metadata,
  PARSE_JSON(SYSTEM$AUTO_REFRESH_STATUS('portfolio_ecommerce_db."portfolio_ecommerce"."orders"')):executionState::STRING AS orders_refresh;
