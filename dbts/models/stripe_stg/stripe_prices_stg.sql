WITH source AS (
    SELECT
        open_id,
        data_json
    FROM {{ source('stripe_stg', 'stripe_raw_prices') }}
),

parsed AS (
    SELECT
        open_id,

        -- ===== SCALAR FIELDS (level 1) =====
        JSON_VALUE(data_json, '$.id')               AS id,
        JSON_VALUE(data_json, '$.object')           AS object,
        CAST(JSON_VALUE(data_json, '$.active') AS BOOL)                    AS active,
        JSON_VALUE(data_json, '$.billing_scheme')   AS billing_scheme,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.created') AS INT64)) AS created,
        JSON_VALUE(data_json, '$.currency')         AS currency,
        CAST(JSON_VALUE(data_json, '$.livemode') AS BOOL)                  AS livemode,
        JSON_VALUE(data_json, '$.lookup_key')       AS lookup_key,
        JSON_VALUE(data_json, '$.nickname')         AS nickname,
        JSON_VALUE(data_json, '$.product')          AS product,
        JSON_VALUE(data_json, '$.tax_behavior')     AS tax_behavior,
        JSON_VALUE(data_json, '$.tiers_mode')       AS tiers_mode,
        JSON_VALUE(data_json, '$.type')             AS type,
        SAFE_CAST(JSON_VALUE(data_json, '$.unit_amount') AS INT64)         AS unit_amount,
        JSON_VALUE(data_json, '$.unit_amount_decimal') AS unit_amount_decimal,

        -- ===== recurring (object → flatten) =====
        JSON_VALUE(data_json, '$.recurring.interval')               AS recurring_interval,
        SAFE_CAST(JSON_VALUE(data_json, '$.recurring.interval_count') AS INT64) AS recurring_interval_count,
        JSON_VALUE(data_json, '$.recurring.meter')                  AS recurring_meter,
        JSON_VALUE(data_json, '$.recurring.usage_type')             AS recurring_usage_type,

        -- ===== custom_unit_amount (object → flatten) =====
        SAFE_CAST(JSON_VALUE(data_json, '$.custom_unit_amount.maximum') AS INT64) AS custom_unit_amount_maximum,
        SAFE_CAST(JSON_VALUE(data_json, '$.custom_unit_amount.minimum') AS INT64) AS custom_unit_amount_minimum,
        SAFE_CAST(JSON_VALUE(data_json, '$.custom_unit_amount.preset') AS INT64)  AS custom_unit_amount_preset,

        -- ===== transform_quantity (object → flatten) =====
        SAFE_CAST(JSON_VALUE(data_json, '$.transform_quantity.divide_by') AS INT64) AS transform_quantity_divide_by,
        JSON_VALUE(data_json, '$.transform_quantity.round')                         AS transform_quantity_round,

        -- ===== metadata — extract specific keys + keep JSON =====
        JSON_VALUE(data_json, '$.metadata.app')        AS metadata_app,
        JSON_VALUE(data_json, '$.metadata.platform')   AS metadata_platform,
        JSON_VALUE(data_json, '$.metadata.product_id') AS metadata_product_id,
        JSON_QUERY(data_json, '$.metadata')            AS metadata,

        -- ===== ARRAY / COMPLEX FIELDS — giữ nguyên JSON =====
        JSON_QUERY(data_json, '$.currency_options')    AS currency_options,
        JSON_QUERY(data_json, '$.tiers')               AS tiers

    FROM source
)

SELECT * FROM parsed