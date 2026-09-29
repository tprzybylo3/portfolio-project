/*
    One row per order, current status only (Spark upserts orders; the
    full status timeline lives in stg_order_status_history).
    Rename + cast only.

    Timestamps: source is TIMESTAMP_LTZ(6) (Spark timestamptz) -> UTC TIMESTAMP_NTZ,
    so values do not depend on the session timezone.
*/

WITH orders AS (

    SELECT * FROM {{ source('portfolio_ecommerce', 'orders') }}

),

renamed_and_typed AS (

    SELECT
        CAST("order_id" AS NUMBER(38, 0))                      AS order_id,
        CAST("customer_id" AS NUMBER(38, 0))                   AS customer_id,
        CAST("status" AS VARCHAR)                              AS order_status,
        CONVERT_TIMEZONE('UTC', "order_date")::TIMESTAMP_NTZ   AS order_date,
        CONVERT_TIMEZONE('UTC', "created_at")::TIMESTAMP_NTZ   AS created_at,
        CONVERT_TIMEZONE('UTC', "updated_at")::TIMESTAMP_NTZ   AS updated_at
    FROM orders

)

SELECT * FROM renamed_and_typed
