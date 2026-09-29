/*
    Current state of each customer: one row per customer_id.

    The source table customer is a full SCD Type 2 table maintained by Spark
    (scripts/merge_dimensions.py). Here we keep only the current version
    (is_current = TRUE). The FULL history stays available in
    {{ source('portfolio_ecommerce', 'customer') }} -- future marts that
    need a historical match should point-in-time join on valid_from/valid_to
    there. It is deliberately NOT rebuilt with dbt snapshot (CLAUDE.md,
    decision 1).

    Source column names are lowercase Glue/Iceberg identifiers, hence the
    double quotes; output aliases are unquoted (standard Snowflake uppercase).

    Timestamps: source is TIMESTAMP_LTZ(6) (Spark timestamptz) -> UTC TIMESTAMP_NTZ,
    so values do not depend on the session timezone.
*/

WITH customer_versions AS (

    SELECT * FROM {{ source('portfolio_ecommerce', 'customer') }}

),

current_customers AS (

    SELECT
        CAST("customer_id" AS NUMBER(38, 0))                   AS customer_id,
        CAST("first_name" AS VARCHAR)                          AS first_name,
        CAST("last_name" AS VARCHAR)                           AS last_name,
        CAST("email" AS VARCHAR)                               AS email,
        CAST("country" AS VARCHAR)                             AS country,
        -- start of the current version; valid_to/is_current are dropped
        -- because after the filter they are always NULL/TRUE
        CONVERT_TIMEZONE('UTC', "valid_from")::TIMESTAMP_NTZ   AS valid_from
    FROM customer_versions
    WHERE "is_current" = TRUE

)

SELECT * FROM current_customers
