/*
    Each customer's SCD2 versions must form a gapless, non-overlapping chain:
    a closed version's valid_to equals the next version's valid_from, and
    only the last version is open (valid_to IS NULL, is_current = TRUE).

    Guards the bug class from PROGRESS_LOG §23 (a ~6 min window with no
    valid version). The per-order test only catches a gap if some order
    happens to fall into it; this one catches every gap.

    Returns offending versions; zero rows = pass.
*/

WITH versions_with_successor AS (

    SELECT
        customer_id,
        version_number,
        valid_from,
        valid_to,
        is_current,
        LEAD(valid_from) OVER (PARTITION BY customer_id ORDER BY version_number) AS next_valid_from
    FROM {{ ref('int_customer_versions') }}

)

SELECT *
FROM versions_with_successor
WHERE
    -- closed version must hand over exactly at the next version's start
    (next_valid_from IS NOT NULL AND valid_to IS DISTINCT FROM next_valid_from)
    -- last version must be the open, current one
    OR (next_valid_from IS NULL AND (valid_to IS NOT NULL OR NOT is_current))
    -- a version window must not be empty or inverted
    OR (valid_to IS NOT NULL AND valid_to <= valid_from)
