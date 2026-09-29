/*
    Current state of each product: one row per product_id.

    The source table product is a full SCD Type 2 table maintained by Spark
    (scripts/merge_dimensions.py). Here we keep only the current version
    (is_current = TRUE). The FULL history (e.g. past prices) stays available
    in {{ source('portfolio_ecommerce', 'product') }} -- future marts that
    need a historical match should point-in-time join on valid_from/valid_to
    there. It is deliberately NOT rebuilt with dbt snapshot (CLAUDE.md,
    decision 1).

    Source column names are lowercase Glue/Iceberg identifiers, hence the
    double quotes; output aliases are unquoted (standard Snowflake uppercase).

    Timestamps: source is TIMESTAMP_LTZ(6) (Spark timestamptz) -> UTC TIMESTAMP_NTZ,
    so values do not depend on the session timezone.
*/

WITH product_versions AS (

    SELECT * FROM {{ source('portfolio_ecommerce', 'product') }}

),

current_products AS (

    SELECT
        CAST("product_id" AS NUMBER(38, 0))                    AS product_id,
        CAST("name" AS VARCHAR)                                AS product_name,
        CAST("category" AS VARCHAR)                            AS category,
        CAST("price" AS NUMBER(10, 2))                         AS price,
        CAST("stock_qty" AS INTEGER)                           AS stock_quantity,
        -- start of the current version; valid_to/is_current are dropped
        -- because after the filter they are always NULL/TRUE
        CONVERT_TIMEZONE('UTC', "valid_from")::TIMESTAMP_NTZ   AS valid_from
    FROM product_versions
    WHERE "is_current" = TRUE

)

SELECT * FROM current_products
