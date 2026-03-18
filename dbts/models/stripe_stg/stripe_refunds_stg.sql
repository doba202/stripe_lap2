WITH source AS (
    SELECT
        id AS raw_id,
        data_json
    FROM {{ source('stripe_stg', 'stripe_raw_refunds') }}
),

parsed AS (
    SELECT
        -- ===== PRIMARY KEY =====
        JSON_VALUE(data_json, '$.id') AS refund_id,

        -- ===== LINK (VERY IMPORTANT 🔥) =====
        JSON_VALUE(data_json, '$.charge') AS charge_id,
        JSON_VALUE(data_json, '$.payment_intent') AS payment_intent_id,
        JSON_VALUE(data_json, '$.balance_transaction') AS balance_txn_id,

        -- ===== AMOUNT =====
        SAFE_CAST(JSON_VALUE(data_json, '$.amount') AS INT64) AS amount,
        JSON_VALUE(data_json, '$.currency') AS currency,

        -- ===== STATUS =====
        JSON_VALUE(data_json, '$.status') AS status,

        -- ===== REASON =====
        JSON_VALUE(data_json, '$.reason') AS reason,

        -- ===== DESCRIPTION =====
        JSON_VALUE(data_json, '$.description') AS description,

        -- ===== CREATED =====
        TIMESTAMP_SECONDS(
            SAFE_CAST(JSON_VALUE(data_json, '$.created') AS INT64)
        ) AS created_at,

        raw_id

    FROM source
)

SELECT * FROM parsed