WITH source AS (
    SELECT
        open_id,call_at,
        available,
        pending,
        refund_and_dispute_prefunding
    FROM {{ source('stripe_stg', 'stripe_raw_balance') }}
),

-- available
available AS (
    SELECT
        open_id,call_at,
        'available' AS type,
        item
    FROM source,
    UNNEST(available) AS item
),

-- pending
pending AS (
    SELECT
        open_id,call_at,
        'pending' AS type,
        item
    FROM source,
    UNNEST(pending) AS item
),

-- prefunding level 1
prefunding_lvl1 AS (
    SELECT
        open_id,call_at,
        pf
    FROM source,
    UNNEST(refund_and_dispute_prefunding) AS pf
),

-- prefunding available
prefunding_available AS (
    SELECT
        open_id,call_at,
        'prefunding_available' AS type,
        item
    FROM prefunding_lvl1,
    UNNEST(JSON_QUERY_ARRAY(pf, '$.available')) AS item
),

-- prefunding pending
prefunding_pending AS (
    SELECT
        open_id,call_at,
        'prefunding_pending' AS type,
        item
    FROM prefunding_lvl1,
    UNNEST(JSON_QUERY_ARRAY(pf, '$.pending')) AS item
),

unioned AS (
    SELECT * FROM available
    UNION ALL
    SELECT * FROM pending
    UNION ALL
    SELECT * FROM prefunding_available
    UNION ALL
    SELECT * FROM prefunding_pending
)

SELECT
    open_id,call_at,
    type,
    JSON_VALUE(item, '$.currency') AS currency,
    CAST(JSON_VALUE(item, '$.amount') AS INT64) AS amount
FROM unioned