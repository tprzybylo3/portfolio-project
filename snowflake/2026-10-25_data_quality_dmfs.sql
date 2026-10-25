-- Step 6.8: custom freshness DMF. Run as ACCOUNTADMIN after the grants script.
-- System FRESHNESS has no TIMESTAMP_NTZ version; DMF bodies must be deterministic,
-- hence DATA_METRIC_SCHEDULED_TIME() instead of CURRENT_TIMESTAMP.
USE ROLE ACCOUNTADMIN;

CREATE SCHEMA IF NOT EXISTS portfolio_governance.data_quality;

CREATE OR REPLACE DATA METRIC FUNCTION portfolio_governance.data_quality.hours_since_latest(
    arg_t TABLE (arg_ts TIMESTAMP_NTZ)
)
RETURNS NUMBER
AS
$$
    SELECT DATEDIFF(
        'hour',
        MAX(arg_ts),
        CONVERT_TIMEZONE('UTC', SNOWFLAKE.CORE.DATA_METRIC_SCHEDULED_TIME())::TIMESTAMP_NTZ
    )
    FROM arg_t
$$;

GRANT USAGE ON SCHEMA portfolio_governance.data_quality TO ROLE portfolio_engineer;
GRANT USAGE ON FUNCTION portfolio_governance.data_quality.hours_since_latest(TABLE (TIMESTAMP_NTZ))
  TO ROLE portfolio_engineer;
