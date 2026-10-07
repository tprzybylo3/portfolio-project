/*
    Reconciliation: total gross value in the mart must equal the total
    over all order lines in staging. Exact decimals on both sides, so no
    tolerance. A mismatch also exposes order lines whose order_id is
    missing from stg_orders (they would be dropped by the mart).

    Returns one row on mismatch; zero rows = pass.
*/

WITH mart_total AS (

    SELECT SUM(gross_order_value) AS gross_value
    FROM {{ ref('fct_orders') }}

),

staging_total AS (

    SELECT SUM(quantity * unit_price) AS gross_value
    FROM {{ ref('stg_order_items') }}

)

SELECT
    mart_total.gross_value                                     AS mart_gross_value,
    staging_total.gross_value                                  AS staging_gross_value
FROM mart_total
CROSS JOIN staging_total
WHERE mart_total.gross_value IS DISTINCT FROM staging_total.gross_value
