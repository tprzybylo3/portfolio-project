/*
    Append-only log of order status transitions, one row per change.
    Rename + cast only.

    Timestamps: source is TIMESTAMP_LTZ(6) (Spark timestamptz) -> UTC TIMESTAMP_NTZ,
    so values do not depend on the session timezone.
*/

WITH status_changes AS (

    SELECT * FROM {{ source('portfolio_ecommerce', 'order_status_history') }}

),

renamed_and_typed AS (

    SELECT
        CAST("history_id" AS NUMBER(38, 0))                    AS history_id,
        CAST("order_id" AS NUMBER(38, 0))                      AS order_id,
        CAST("status" AS VARCHAR)                              AS order_status,
        CONVERT_TIMEZONE('UTC', "changed_at")::TIMESTAMP_NTZ   AS changed_at
    FROM status_changes

)

SELECT * FROM renamed_and_typed
