{{ config(unique_key=['id', 'open_id']) }}

WITH source AS (
    SELECT
        open_id,
        data_json
    FROM {{ source('stripe_stg', 'stripe_raw_products') }}
    WHERE 1=1
    {% if is_incremental() %}
        AND TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.created') AS INT64))
            >= TIMESTAMP_SECONDS({{ var('stripe_start_ts') }})
    {% endif %}
),

parsed AS (
    SELECT
        open_id,

        -- ===== SCALAR FIELDS (level 1) =====
        JSON_VALUE(data_json, '$.id')                   AS id,
        JSON_VALUE(data_json, '$.object')               AS object,
        CAST(JSON_VALUE(data_json, '$.active') AS BOOL) AS active,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.created') AS INT64)) AS created,
        JSON_VALUE(data_json, '$.default_price')        AS default_price,
        JSON_VALUE(data_json, '$.description')          AS description,
        CAST(JSON_VALUE(data_json, '$.livemode') AS BOOL) AS livemode,
        JSON_VALUE(data_json, '$.name')                 AS name,
        CAST(JSON_VALUE(data_json, '$.shippable') AS BOOL) AS shippable,
        JSON_VALUE(data_json, '$.statement_descriptor') AS statement_descriptor,
        JSON_VALUE(data_json, '$.tax_code')             AS tax_code,
        JSON_VALUE(data_json, '$.unit_label')           AS unit_label,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.updated') AS INT64)) AS updated,
        JSON_VALUE(data_json, '$.url')                  AS url,

        -- ===== package_dimensions (object → flatten) =====
        SAFE_CAST(JSON_VALUE(data_json, '$.package_dimensions.height') AS FLOAT64) AS package_dimensions_height,
        SAFE_CAST(JSON_VALUE(data_json, '$.package_dimensions.length') AS FLOAT64) AS package_dimensions_length,
        SAFE_CAST(JSON_VALUE(data_json, '$.package_dimensions.weight') AS FLOAT64) AS package_dimensions_weight,
        SAFE_CAST(JSON_VALUE(data_json, '$.package_dimensions.width') AS FLOAT64)  AS package_dimensions_width,

        -- ===== metadata — extract specific keys + keep JSON =====
        JSON_VALUE(data_json, '$.metadata.app')        AS metadata_app,
        JSON_VALUE(data_json, '$.metadata.platform')   AS metadata_platform,
        JSON_VALUE(data_json, '$.metadata.product_id') AS metadata_product_id,
        JSON_QUERY(data_json, '$.metadata')            AS metadata,

        -- ===== ARRAY FIELDS — giữ nguyên JSON =====
        JSON_QUERY(data_json, '$.images')               AS images,
        JSON_QUERY(data_json, '$.marketing_features')   AS marketing_features

    FROM source
)

SELECT * FROM parsed