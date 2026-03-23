WITH source AS (
    SELECT
        open_id,
        data_json
    FROM {{ source('stripe_stg', 'stripe_raw_products') }}
),

parsed AS (
    SELECT
        open_id,
        -- ===== PRIMARY =====
        JSON_VALUE(data_json, '$.id') AS product_id,

        -- ===== BASIC INFO =====
        JSON_VALUE(data_json, '$.name') AS name,
        JSON_VALUE(data_json, '$.description') AS description,
        CAST(JSON_VALUE(data_json, '$.active') AS BOOL) AS is_active,
        CAST(JSON_VALUE(data_json, '$.livemode') AS BOOL) AS is_livemode,

        -- ===== PRICE =====
        JSON_VALUE(data_json, '$.default_price') AS default_price_id,

        -- ===== SHIPPING =====
        CAST(JSON_VALUE(data_json, '$.shippable') AS BOOL) AS is_shippable,
        SAFE_CAST(JSON_VALUE(data_json, '$.package_dimensions.height') AS FLOAT64) AS package_height,
        SAFE_CAST(JSON_VALUE(data_json, '$.package_dimensions.length') AS FLOAT64) AS package_length,
        SAFE_CAST(JSON_VALUE(data_json, '$.package_dimensions.weight') AS FLOAT64) AS package_weight,
        SAFE_CAST(JSON_VALUE(data_json, '$.package_dimensions.width') AS FLOAT64) AS package_width,

        -- ===== DISPLAY =====
        JSON_VALUE(data_json, '$.statement_descriptor') AS statement_descriptor,
        JSON_VALUE(data_json, '$.unit_label') AS unit_label,
        JSON_VALUE(data_json, '$.url') AS url,

        -- ===== TAX =====
        JSON_VALUE(data_json, '$.tax_code') AS tax_code_id,

        -- ===== IMAGES =====
        ARRAY(
            SELECT JSON_VALUE(x)
            FROM UNNEST(JSON_QUERY_ARRAY(data_json, '$.images')) AS x
        ) AS images,

        -- ===== MARKETING FEATURES =====
        ARRAY(
            SELECT JSON_VALUE(mf, '$.name')
            FROM UNNEST(JSON_QUERY_ARRAY(data_json, '$.marketing_features')) AS mf
        ) AS marketing_feature_names,

        -- ===== METADATA =====
        JSON_QUERY(data_json, '$.metadata') AS metadata_json,

        -- ===== DATES =====
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.created') AS INT64))
            AS created_at,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.updated') AS INT64))
            AS updated_at

    FROM source
)

SELECT * FROM parsed