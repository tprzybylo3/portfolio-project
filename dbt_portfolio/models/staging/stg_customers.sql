/*
    All SCD Type 2 versions of each customer: one row per
    (customer_id, valid_from).

    The source table customer is a full SCD Type 2 table maintained by Spark
    (scripts/merge_dimensions.py). Staging passes every version through;
    consumers pick the current state with is_current = TRUE, or a
    historical match by point-in-time join on valid_from/valid_to
    (see int_customer_versions). It is deliberately NOT rebuilt with dbt
    snapshot (CLAUDE.md, decision 1).

    Source column names are lowercase Glue/Iceberg identifiers, hence the
    double quotes; output aliases are unquoted (standard Snowflake uppercase).

    Timestamps: source is TIMESTAMP_LTZ(6) (verified with DESCRIBE TABLE) -> UTC TIMESTAMP_NTZ,
    so values do not depend on the session timezone.
*/

WITH customer_versions AS (

    SELECT * FROM {{ source('portfolio_ecommerce', 'customer') }}

),

renamed_and_typed AS (

    SELECT
        CAST("customer_id" AS NUMBER(38, 0))                   AS customer_id,
        CAST("first_name" AS VARCHAR)                          AS first_name,
        CAST("last_name" AS VARCHAR)                           AS last_name,
        CAST("email" AS VARCHAR)                               AS email,
        CAST("country" AS VARCHAR)                             AS country,
        -- version window is half-open [valid_from, valid_to): Spark sets the
        -- closed version's valid_to to the next version's valid_from
        CONVERT_TIMEZONE('UTC', "valid_from")::TIMESTAMP_NTZ   AS valid_from,
        CONVERT_TIMEZONE('UTC', "valid_to")::TIMESTAMP_NTZ     AS valid_to,
        CAST("is_current" AS BOOLEAN)                          AS is_current
    FROM customer_versions

)

SELECT * FROM renamed_and_typed
