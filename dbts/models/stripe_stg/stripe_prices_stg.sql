WITH source AS (
    SELECT
        id AS raw_id,
        data_json
    FROM {{ source('stripe_stg', 'stripe_raw_prices') }}
),

parsed AS (
    SELECT
        -- ===== PRIMARY KEY =====
        JSON_VALUE(data_json, '$.id') AS price_id,

        -- ===== PRODUCT =====
        JSON_VALUE(data_json, '$.product') AS product_id,

        -- ===== STATUS =====
        CAST(JSON_VALUE(data_json, '$.active') AS BOOL) AS is_active,

        -- ===== BASIC =====
        JSON_VALUE(data_json, '$.currency') AS currency,
        JSON_VALUE(data_json, '$.nickname') AS nickname,

        -- ===== TYPE =====
        JSON_VALUE(data_json, '$.type') AS price_type,  -- one_time / recurring

        -- ===== AMOUNT =====
        SAFE_CAST(JSON_VALUE(data_json, '$.unit_amount') AS INT64) AS unit_amount,

        -- ===== RECURRING =====
        JSON_VALUE(data_json, '$.recurring.interval') AS billing_interval,
        SAFE_CAST(JSON_VALUE(data_json, '$.recurring.interval_count') AS INT64) AS interval_count,
        JSON_VALUE(data_json, '$.recurring.usage_type') AS usage_type,
        JSON_VALUE(data_json, '$.recurring.meter') AS meter,

        -- ===== TAX =====
        JSON_VALUE(data_json, '$.tax_behavior') AS tax_behavior,

        -- ===== METADATA =====
        JSON_VALUE(data_json, '$.metadata.productId') AS metadata_product_id,

        -- debug
        raw_id

    FROM source
)

SELECT * FROM parsed