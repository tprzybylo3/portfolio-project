-- Account setup (phase 4), read back from the trial account on 2026-10-25 before it expired.
-- Run as ACCOUNTADMIN on a new account. Replace <AWS_ACCOUNT_ID> and <SNOWFLAKE_USER>.
--
-- Run order:
--   1. this file
--   2. AWS: put each object's STORAGE_AWS_IAM_USER_ARN / API_AWS_IAM_USER_ARN and external ID
--      (DESC EXTERNAL VOLUME ..., DESC CATALOG INTEGRATION ...) into the IAM role trust policies
--   3. 2026-10-22_recreate_catalog_linked_db.sql  (catalog-linked DB, email_mask on customer)
--   4. phase5_gold_governance.sql                 (country_mask, gold grants)
--   5. 2026-10-25_data_quality_grants.sql, 2026-10-25_data_quality_dmfs.sql
--   6. dbt build

USE ROLE ACCOUNTADMIN;

-- roles: engineer inherits analyst; both under SYSADMIN
CREATE ROLE IF NOT EXISTS portfolio_analyst;
CREATE ROLE IF NOT EXISTS portfolio_engineer;
GRANT ROLE portfolio_analyst TO ROLE portfolio_engineer;
GRANT ROLE portfolio_engineer TO ROLE SYSADMIN;
GRANT ROLE portfolio_engineer TO USER <SNOWFLAKE_USER>;
-- dbt connects as this user with key-pair auth (no password); a dedicated service user is phase 7
-- ALTER USER <SNOWFLAKE_USER> SET RSA_PUBLIC_KEY = '<public key>';

-- warehouse (COMPUTE_WH exists on a new account; settings as on the trial)
CREATE WAREHOUSE IF NOT EXISTS compute_wh
  WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 600 AUTO_RESUME = TRUE;
GRANT USAGE ON WAREHOUSE compute_wh TO ROLE portfolio_analyst;

-- databases
CREATE DATABASE IF NOT EXISTS portfolio_dbt;
CREATE DATABASE IF NOT EXISTS portfolio_governance;
CREATE SCHEMA IF NOT EXISTS portfolio_governance.policies;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE portfolio_engineer;
GRANT USAGE, CREATE SCHEMA ON DATABASE portfolio_dbt TO ROLE portfolio_engineer;

-- policies (country_mask is in phase5_gold_governance.sql)
CREATE OR REPLACE ROW ACCESS POLICY portfolio_governance.policies.country_rls
  AS (country_col VARCHAR) RETURNS BOOLEAN ->
  CASE
    WHEN CURRENT_ROLE() IN ('PORTFOLIO_ENGINEER', 'ACCOUNTADMIN') THEN TRUE
    WHEN CURRENT_ROLE() = 'PORTFOLIO_ANALYST' AND country_col = 'DE' THEN TRUE
    ELSE FALSE
  END;

CREATE OR REPLACE MASKING POLICY portfolio_governance.policies.email_mask
  AS (val VARCHAR) RETURNS VARCHAR ->
  CASE
    WHEN CURRENT_ROLE() IN ('PORTFOLIO_ENGINEER', 'ACCOUNTADMIN') THEN val
    ELSE REGEXP_REPLACE(val, '^[^@]+', '***')
  END;

-- external volumes: silver read-only, gold writable (Snowflake-managed Iceberg marts)
CREATE EXTERNAL VOLUME IF NOT EXISTS portfolio_ecommerce_ext_vol
  STORAGE_LOCATIONS = ((
    NAME = 'tp-portfolio-ecommerce-raw-loc'
    STORAGE_PROVIDER = 'S3'
    STORAGE_BASE_URL = 's3://tp-portfolio-ecommerce-raw/'
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<AWS_ACCOUNT_ID>:role/snowflake-s3-external-volume-role'
    STORAGE_AWS_EXTERNAL_ID = 'portfolio_ecommerce_ext_vol_eid'
  ))
  ALLOW_WRITES = FALSE;

CREATE EXTERNAL VOLUME IF NOT EXISTS portfolio_gold_ext_vol
  STORAGE_LOCATIONS = ((
    NAME = 'tp-portfolio-gold-loc'
    STORAGE_PROVIDER = 'S3'
    STORAGE_BASE_URL = 's3://tp-portfolio-ecommerce-raw/gold/'
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<AWS_ACCOUNT_ID>:role/snowflake-s3-gold-write-role'
    STORAGE_AWS_EXTERNAL_ID = 'portfolio_gold_ext_vol_eid'
  ))
  ALLOW_WRITES = TRUE;

-- Glue Iceberg REST catalog (ADR-004)
CREATE CATALOG INTEGRATION IF NOT EXISTS glue_rest_catalog_int
  CATALOG_SOURCE = ICEBERG_REST
  TABLE_FORMAT = ICEBERG
  CATALOG_NAMESPACE = 'portfolio_ecommerce'
  REST_CONFIG = (
    CATALOG_URI = 'https://glue.eu-central-1.amazonaws.com/iceberg'
    CATALOG_API_TYPE = AWS_GLUE
    CATALOG_NAME = '<AWS_ACCOUNT_ID>'
    ACCESS_DELEGATION_MODE = EXTERNAL_VOLUME_CREDENTIALS
  )
  REST_AUTHENTICATION = (
    TYPE = SIGV4
    SIGV4_IAM_ROLE = 'arn:aws:iam::<AWS_ACCOUNT_ID>:role/snowflake-glue-catalog-role'
    SIGV4_SIGNING_REGION = 'eu-central-1'
  )
  REFRESH_INTERVAL_SECONDS = 30
  ENABLED = TRUE;

GRANT USAGE ON INTEGRATION glue_rest_catalog_int TO ROLE portfolio_engineer;
GRANT USAGE ON EXTERNAL VOLUME portfolio_ecommerce_ext_vol TO ROLE portfolio_engineer;
GRANT USAGE ON EXTERNAL VOLUME portfolio_gold_ext_vol TO ROLE portfolio_engineer;
