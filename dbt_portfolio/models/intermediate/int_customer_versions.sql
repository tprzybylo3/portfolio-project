/*
    Customer SCD2 versions with effective windows for point-in-time joins.

    Facts match a version with
        effective_from <= event_ts AND event_ts < effective_to
    (half-open, same semantics as Spark's valid_from/valid_to).

    Business rule: the FIRST version of each customer is effective from
    1900-01-01. Seed data (scripts/seed.py) draws order dates independently
    of customer creation dates, so 115 of 200 orders precede the customer's
    first version (PROGRESS_LOG §23) -- not a pipeline bug, and the source is
    deliberately not "fixed". Without this rule those orders would have no
    country_at_order. valid_from itself is kept unchanged for auditing.

    effective_to comes from Spark's valid_to (not LEAD(valid_from)) on
    purpose: a gap between versions must surface in tests, not be papered
    over here.

    email is deliberately not carried forward -- marts do not need it, and
    leaving it out keeps the masked column out of Gold.
*/

WITH customer_versions AS (

    SELECT
        customer_id,
        country,
        valid_from,
        valid_to,
        is_current
    FROM {{ ref('stg_customers') }}

),

numbered_versions AS (

    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY valid_from) AS version_number
    FROM customer_versions

),

effective_windows AS (

    SELECT
        customer_id,
        country,
        valid_from,
        valid_to,
        is_current,
        version_number,
        version_number = 1                                     AS is_first_version,
        CASE
            WHEN version_number = 1 THEN '1900-01-01'::TIMESTAMP_NTZ
            ELSE valid_from
        END                                                    AS effective_from,
        COALESCE(valid_to, '9999-12-31'::TIMESTAMP_NTZ)        AS effective_to
    FROM numbered_versions

)

SELECT * FROM effective_windows
