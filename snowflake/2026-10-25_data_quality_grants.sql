-- Step 6.8: privileges for the DMFs attached by dbt post_hooks on the marts. Run as ACCOUNTADMIN.
-- DMFs run as the table owner (portfolio_engineer), which RLS does not restrict.
USE ROLE ACCOUNTADMIN;
GRANT EXECUTE DATA METRIC FUNCTION ON ACCOUNT TO ROLE portfolio_engineer;
GRANT APPLICATION ROLE SNOWFLAKE.DATA_QUALITY_MONITORING_VIEWER TO ROLE portfolio_engineer;
