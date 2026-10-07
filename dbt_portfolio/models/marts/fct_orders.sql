/*
    One row per order: value, payment and customer geography at order time.

    Gold layer: Snowflake-managed Iceberg table (catalog gold_iceberg,
    see catalogs.yml / dbt_project.yml). Recreated on every run, so both
    governance policies are re-attached by the post_hooks below -- a manual
    ALTER would be lost:
      - row access policy country_rls on country_at_order,
      - masking policy country_mask on current_country (RLS filters by
        country at order time; a customer who has since moved would
        otherwise leak their non-DE current country to the analyst).
    dbt runs post_hooks BEFORE applying grants, so portfolio_analyst never
    gets SELECT on a copy without the policies.

    country_at_order: point-in-time match to the customer version effective
    at order_date (int_customer_versions; first version backdated to
    1900-01-01, see that model). current_country: today's version.

    LEFT JOINs throughout: an unmatched order must surface as NULL and fail
    a test, not silently disappear from the mart.

    Timestamps are cast to TIMESTAMP_NTZ(6) on output: Snowflake-managed
    Iceberg rejects staging's TIMESTAMP_NTZ(9) ("Invalid time type scale",
    verified on the first dbt run). No data is lost -- the source is
    TIMESTAMP_LTZ(6), so values never carry more than microseconds.
*/

{{
    config(
        post_hook=[
            "ALTER ICEBERG TABLE {{ this }} ADD ROW ACCESS POLICY portfolio_governance.policies.country_rls ON (country_at_order)",
            "ALTER ICEBERG TABLE {{ this }} MODIFY COLUMN current_country SET MASKING POLICY portfolio_governance.policies.country_mask",
        ]
    )
}}

WITH orders AS (

    SELECT
        order_id,
        customer_id,
        order_date,
        order_status
    FROM {{ ref('stg_orders') }}

),

order_line_totals AS (

    SELECT
        order_id,
        SUM(quantity * unit_price)                             AS gross_order_value
    FROM {{ ref('stg_order_items') }}
    GROUP BY order_id

),

payment_totals_by_status AS (

    -- paid_amount: net settled amount -- 'completed' payments only; a
    -- refunded payment is excluded here and reported in refunded_amount,
    -- failed payments in neither
    SELECT
        order_id,
        SUM(CASE WHEN payment_status = 'completed' THEN amount END) AS paid_amount,
        SUM(CASE WHEN payment_status = 'refunded'  THEN amount END) AS refunded_amount
    FROM {{ ref('stg_payments') }}
    GROUP BY order_id

),

customer_version_at_order AS (

    SELECT
        orders.order_id,
        customer_versions.country                              AS country_at_order,
        -- order predates the customer's real first version and matched only
        -- thanks to the 1900-01-01 backdating rule
        orders.order_date < customer_versions.valid_from       AS is_before_first_customer_version
    FROM orders
    INNER JOIN {{ ref('int_customer_versions') }} AS customer_versions
        ON  customer_versions.customer_id = orders.customer_id
        AND orders.order_date >= customer_versions.effective_from
        AND orders.order_date <  customer_versions.effective_to

),

current_customer_country AS (

    SELECT
        customer_id,
        country                                                AS current_country
    FROM {{ ref('int_customer_versions') }}
    WHERE is_current

),

orders_enriched AS (

    SELECT
        orders.order_id,
        orders.customer_id,
        orders.order_date::TIMESTAMP_NTZ(6)                    AS order_date,
        orders.order_status,
        customer_version_at_order.country_at_order,
        current_customer_country.current_country,
        customer_version_at_order.is_before_first_customer_version,
        COALESCE(order_line_totals.gross_order_value, 0)       AS gross_order_value,
        COALESCE(payment_totals_by_status.paid_amount, 0)      AS paid_amount,
        COALESCE(payment_totals_by_status.refunded_amount, 0)  AS refunded_amount
    FROM orders
    LEFT JOIN customer_version_at_order
        ON customer_version_at_order.order_id = orders.order_id
    LEFT JOIN current_customer_country
        ON current_customer_country.customer_id = orders.customer_id
    LEFT JOIN order_line_totals
        ON order_line_totals.order_id = orders.order_id
    LEFT JOIN payment_totals_by_status
        ON payment_totals_by_status.order_id = orders.order_id

)

SELECT * FROM orders_enriched
