WITH source AS (
    SELECT
        open_id,
        data_json
    FROM {{ source('stripe_stg', 'stripe_raw_products') }}
),

parsed AS (
    SELECT
        open_id,
        -- ===== PRIMARY KEY =====
        JSON_VALUE(data_json, '$.id') AS product_id,

        -- ===== BASIC =====
        JSON_VALUE(data_json, '$.name') AS product_name,
        JSON_VALUE(data_json, '$.description') AS description,

        -- ===== STATUS =====
        CAST(JSON_VALUE(data_json, '$.active') AS BOOL) AS is_active,

        -- ===== PRICE =====
        JSON_VALUE(data_json, '$.default_price') AS default_price_id,

        -- ===== TAX =====
        JSON_VALUE(data_json, '$.tax_code') AS tax_code,

        -- ===== TYPE =====
        JSON_VALUE(data_json, '$.type') AS product_type,

        -- ===== CREATED / UPDATED =====
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.created') AS INT64)) AS created_at,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.updated') AS INT64)) AS updated_at,

        -- ===== OPTIONAL =====
        CAST(JSON_VALUE(data_json, '$.shippable') AS BOOL) AS is_shippable,
        JSON_VALUE(data_json, '$.unit_label') AS unit_label,

        -- ===== METADATA (expand nếu cần) =====
        --JSON_VALUE(data_json, '$.metadata.xxx') AS metadata_xxx,
    FROM source
)

SELECT * FROM parsed