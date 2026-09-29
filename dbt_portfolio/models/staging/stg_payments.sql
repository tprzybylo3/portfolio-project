/*
    One row per payment, current state (Spark upserts payments).
    Rename + cast only.

    Timestamps: source is TIMESTAMP_LTZ(6) (Spark timestamptz) -> UTC TIMESTAMP_NTZ,
    so values do not depend on the session timezone.
*/

WITH payments AS (

    SELECT * FROM {{ source('portfolio_ecommerce', 'payments') }}

),

renamed_and_typed AS (

    SELECT
        CAST("payment_id" AS NUMBER(38, 0))                    AS payment_id,
        CAST("order_id" AS NUMBER(38, 0))                      AS order_id,
        CAST("amount" AS NUMBER(10, 2))                        AS amount,
        CAST("method" AS VARCHAR)                              AS payment_method,
        CAST("status" AS VARCHAR)                              AS payment_status,
        CONVERT_TIMEZONE('UTC', "created_at")::TIMESTAMP_NTZ   AS created_at,
        CONVERT_TIMEZONE('UTC', "updated_at")::TIMESTAMP_NTZ   AS updated_at
    FROM payments

)

SELECT * FROM renamed_and_typed
