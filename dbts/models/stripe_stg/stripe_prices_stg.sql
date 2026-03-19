WITH source AS (
    SELECT
        open_id,
        data_json
    FROM {{ source('stripe_stg', 'stripe_raw_prices') }}
),

parsed AS (
    SELECT
        open_id,
        -- ===== PRIMARY =====
        JSON_VALUE(data_json, '$.id') AS price_id,
        JSON_VALUE(data_json, '$.product') AS product_id,

        -- ===== STATUS =====
        CAST(JSON_VALUE(data_json, '$.active') AS BOOL) AS is_active,

        -- ===== AMOUNT =====
        SAFE_CAST(JSON_VALUE(data_json, '$.unit_amount') AS INT64) AS unit_amount,
        SAFE_CAST(JSON_VALUE(data_json, '$.unit_amount_decimal') AS NUMERIC) AS unit_amount_decimal,
        JSON_VALUE(data_json, '$.currency') AS currency,

        -- ===== TYPE =====
        JSON_VALUE(data_json, '$.type') AS price_type,

        -- ===== BILLING =====
        JSON_VALUE(data_json, '$.billing_scheme') AS billing_scheme,
        JSON_VALUE(data_json, '$.tiers_mode') AS tiers_mode,

        -- ===== RECURRING =====
        JSON_VALUE(data_json, '$.recurring.interval') AS recurring_interval,
        SAFE_CAST(JSON_VALUE(data_json, '$.recurring.interval_count') AS INT64) AS recurring_interval_count,
        JSON_VALUE(data_json, '$.recurring.usage_type') AS recurring_usage_type,

        -- ===== OPTIONAL =====
        JSON_VALUE(data_json, '$.nickname') AS nickname,
        JSON_VALUE(data_json, '$.tax_behavior') AS tax_behavior,

        -- ===== CREATED =====
        TIMESTAMP_SECONDS(
            SAFE_CAST(JSON_VALUE(data_json, '$.created') AS INT64)
        ) AS created_at

    FROM source
)

SELECT * FROM parsed