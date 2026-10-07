-- Phase 5 (dbt Gold): Snowflake objects created MANUALLY, outside dbt.
-- dbt attaches the policies (post_hook) and grants SELECT on the mart
-- (grants config); everything below must exist beforehand.
-- Executed by hand on 2026-10-07. See PROGRESS_LOG §24.

USE ROLE ACCOUNTADMIN;

-- portfolio_engineer runs dbt and attaches policies in the post_hook
GRANT USAGE ON DATABASE portfolio_governance TO ROLE portfolio_engineer;
GRANT USAGE ON SCHEMA portfolio_governance.policies TO ROLE portfolio_engineer;
GRANT APPLY ON ROW ACCESS POLICY portfolio_governance.policies.country_rls TO ROLE portfolio_engineer;

-- Codex P1: current_country could reveal a country hidden by country_rls
USE DATABASE portfolio_governance;
USE SCHEMA policies;
CREATE MASKING POLICY portfolio_governance.policies.country_mask
    AS (country VARCHAR) RETURNS VARCHAR ->
    CASE
        WHEN CURRENT_ROLE() IN ('PORTFOLIO_ENGINEER', 'ACCOUNTADMIN') THEN country
        WHEN CURRENT_ROLE() = 'PORTFOLIO_ANALYST' AND country = 'DE' THEN country
        ELSE NULL
    END;
GRANT APPLY ON MASKING POLICY portfolio_governance.policies.country_mask TO ROLE portfolio_engineer;

-- analyst access to the marts schema (dbt grants cover the table only;
-- the schema exists only after the first dbt run)
GRANT USAGE ON DATABASE portfolio_dbt TO ROLE portfolio_analyst;
GRANT USAGE ON SCHEMA portfolio_dbt.dbt_dev_marts TO ROLE portfolio_analyst;
