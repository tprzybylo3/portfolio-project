/*
    Every order must match EXACTLY ONE customer version at order_date:
    0 matches = orphan (no country_at_order), >1 = overlapping versions
    (would fan out the mart's grain).

    Recomputed independently from staging + int_customer_versions rather
    than read from fct_orders, so a bug in the mart's own join cannot hide
    itself.

    Returns offending orders; zero rows = pass.
*/

WITH orders AS (

    SELECT order_id, customer_id, order_date
    FROM {{ ref('stg_orders') }}

),

matched_versions_per_order AS (

    SELECT
        orders.order_id,
        COUNT(customer_versions.customer_id)                   AS matched_version_count
    FROM orders
    LEFT JOIN {{ ref('int_customer_versions') }} AS customer_versions
        ON  customer_versions.customer_id = orders.customer_id
        AND orders.order_date >= customer_versions.effective_from
        AND orders.order_date <  customer_versions.effective_to
    GROUP BY orders.order_id

)

SELECT *
FROM matched_versions_per_order
WHERE matched_version_count <> 1
