WITH source AS (
    SELECT
        open_id,
        data_json
    FROM {{ source('stripe_stg', 'stripe_raw_balance_transactions') }}
),

parsed AS (
    SELECT
        open_id,

        -- ===== SCALAR FIELDS (level 1) =====
        JSON_VALUE(data_json, '$.id')                 AS id,
        JSON_VALUE(data_json, '$.object')             AS object,
        SAFE_CAST(JSON_VALUE(data_json, '$.amount') AS INT64)        AS amount,
        SAFE_CAST(JSON_VALUE(data_json, '$.fee') AS INT64)           AS fee,
        SAFE_CAST(JSON_VALUE(data_json, '$.net') AS INT64)           AS net,
        JSON_VALUE(data_json, '$.currency')           AS currency,
        SAFE_CAST(JSON_VALUE(data_json, '$.exchange_rate') AS FLOAT64) AS exchange_rate,
        JSON_VALUE(data_json, '$.type')               AS type,
        JSON_VALUE(data_json, '$.reporting_category') AS reporting_category,
        JSON_VALUE(data_json, '$.balance_type')       AS balance_type,
        JSON_VALUE(data_json, '$.status')             AS status,
        JSON_VALUE(data_json, '$.description')        AS description,
        JSON_VALUE(data_json, '$.source')             AS source,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.created')      AS INT64)) AS created,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.available_on') AS INT64)) AS available_on,

        -- ===== ARRAY / OBJECT FIELDS (level 1 — giữ nguyên JSON) =====
        JSON_QUERY(data_json, '$.fee_details') AS fee_details

    FROM source
)

SELECT * FROM parsed