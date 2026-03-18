WITH source AS (
    SELECT
        id AS raw_id,
        data_json
    FROM {{ source('stripe_stg', 'stripe_raw_balance_transactions') }}
),

parsed AS (
    SELECT
        -- ===== PRIMARY KEY =====
        JSON_VALUE(data_json, '$.id') AS balance_txn_id,

        -- ===== AMOUNT =====
        SAFE_CAST(JSON_VALUE(data_json, '$.amount') AS INT64) AS amount,
        SAFE_CAST(JSON_VALUE(data_json, '$.fee') AS INT64) AS fee,
        SAFE_CAST(JSON_VALUE(data_json, '$.net') AS INT64) AS net,

        JSON_VALUE(data_json, '$.currency') AS currency,

        -- ===== TYPE =====
        JSON_VALUE(data_json, '$.type') AS txn_type,
        JSON_VALUE(data_json, '$.reporting_category') AS reporting_category,
        JSON_VALUE(data_json, '$.balance_type') AS balance_type,

        -- ===== STATUS =====
        JSON_VALUE(data_json, '$.status') AS status,

        -- ===== DESCRIPTION =====
        JSON_VALUE(data_json, '$.description') AS description,

        -- ===== SOURCE (IMPORTANT) =====
        JSON_VALUE(data_json, '$.source') AS source_id,

        -- ===== TIMESTAMP =====
        TIMESTAMP_SECONDS(
            SAFE_CAST(JSON_VALUE(data_json, '$.created') AS INT64)
        ) AS created_at,

        TIMESTAMP_SECONDS(
            SAFE_CAST(JSON_VALUE(data_json, '$.available_on') AS INT64)
        ) AS available_on,

        raw_id

    FROM source
)

SELECT * FROM parsed