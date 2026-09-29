/*
    One row per order line. Rename + cast only.

    Timestamps: source is TIMESTAMP_LTZ(6) (Spark timestamptz) -> UTC TIMESTAMP_NTZ,
    so values do not depend on the session timezone.
*/

WITH order_items AS (

    SELECT * FROM {{ source('portfolio_ecommerce', 'order_items') }}

),

renamed_and_typed AS (

    SELECT
        CAST("order_item_id" AS NUMBER(38, 0))                 AS order_item_id,
        CAST("order_id" AS NUMBER(38, 0))                      AS order_id,
        CAST("product_id" AS NUMBER(38, 0))                    AS product_id,
        CAST("qty" AS INTEGER)                                 AS quantity,
        CAST("unit_price" AS NUMBER(10, 2))                    AS unit_price,
        CONVERT_TIMEZONE('UTC', "created_at")::TIMESTAMP_NTZ   AS created_at,
        CONVERT_TIMEZONE('UTC', "updated_at")::TIMESTAMP_NTZ   AS updated_at
    FROM order_items

)

SELECT * FROM renamed_and_typed
