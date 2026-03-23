WITH source AS (
    SELECT
        open_id,
        call_at,
        available,
        pending,
        connect_reserved,
        refund_and_dispute_prefunding
    FROM {{ source('stripe_stg', 'stripe_raw_balance') }}
),

-- ===== available =====
available AS (
    SELECT
        open_id,
        call_at,
        'available' AS type,
        item
    FROM source,
    UNNEST(IFNULL(available, [])) AS item
),

-- ===== pending =====
pending AS (
    SELECT
        open_id,
        call_at,
        'pending' AS type,
        item
    FROM source,
    UNNEST(IFNULL(pending, [])) AS item
),

-- ===== connect_reserved =====
connect_reserved AS (
    SELECT
        open_id,
        call_at,
        'connect_reserved' AS type,
        item
    FROM source,
    UNNEST(IFNULL(connect_reserved, [])) AS item
),

-- ===== prefunding (JSON object) =====
prefunding AS (
    SELECT
        open_id,
        call_at,
        refund_and_dispute_prefunding AS pf
    FROM source
),

-- ===== prefunding.available =====
prefunding_available AS (
    SELECT
        open_id,
        call_at,
        'prefunding_available' AS type,
        item
    FROM prefunding,
    UNNEST(IFNULL(JSON_QUERY_ARRAY(pf, '$.available'), [])) AS item
),

-- ===== prefunding.pending =====
prefunding_pending AS (
    SELECT
        open_id,
        call_at,
        'prefunding_pending' AS type,
        item
    FROM prefunding,
    UNNEST(IFNULL(JSON_QUERY_ARRAY(pf, '$.pending'), [])) AS item
),

-- ===== UNION ALL =====
unioned AS (
    SELECT * FROM available
    UNION ALL
    SELECT * FROM pending
    UNION ALL
    SELECT * FROM connect_reserved
    UNION ALL
    SELECT * FROM prefunding_available
    UNION ALL
    SELECT * FROM prefunding_pending
)

-- ===== FINAL =====
SELECT
    open_id,
    call_at,
    type,
    JSON_VALUE(item, '$.currency') AS currency,
    CAST(JSON_VALUE(item, '$.amount') AS INT64) AS amount,

    -- source_types breakdown (optional)
    CAST(JSON_VALUE(item, '$.source_types.card') AS INT64) AS card_amount,
    CAST(JSON_VALUE(item, '$.source_types.bank_account') AS INT64) AS bank_amount,
    CAST(JSON_VALUE(item, '$.source_types.fpx') AS INT64) AS fpx_amount

FROM unioned