WITH source AS (
    SELECT
        id AS raw_id,
        data_json
    FROM {{ source('stripe_stg', 'stripe_raw_plans') }}
),

parsed AS (
    SELECT
        -- ===== PRIMARY KEY =====
        JSON_VALUE(data_json, '$.id') AS plan_id,

        -- ===== PRODUCT =====
        JSON_VALUE(data_json, '$.product') AS product_id,

        -- ===== STATUS =====
        CAST(JSON_VALUE(data_json, '$.active') AS BOOL) AS is_active,

        -- ===== AMOUNT =====
        SAFE_CAST(JSON_VALUE(data_json, '$.amount') AS INT64) AS amount,
        SAFE_CAST(JSON_VALUE(data_json, '$.amount_decimal') AS NUMERIC) AS amount_decimal,
        JSON_VALUE(data_json, '$.currency') AS currency,

        -- ===== BILLING =====
        JSON_VALUE(data_json, '$.billing_scheme') AS billing_scheme,
        JSON_VALUE(data_json, '$.usage_type') AS usage_type,

        -- ===== INTERVAL =====
        JSON_VALUE(data_json, '$.interval') AS `interval`,
        SAFE_CAST(JSON_VALUE(data_json, '$.interval_count') AS INT64) AS interval_count,

        -- ===== OPTIONAL =====
        JSON_VALUE(data_json, '$.nickname') AS nickname,
        JSON_VALUE(data_json, '$.tiers_mode') AS tiers_mode,
        JSON_VALUE(data_json, '$.meter') AS meter,

        -- ===== TRIAL =====
        SAFE_CAST(JSON_VALUE(data_json, '$.trial_period_days') AS INT64) AS trial_period_days,

        -- ===== CREATED =====
        TIMESTAMP_SECONDS(
            SAFE_CAST(JSON_VALUE(data_json, '$.created') AS INT64)
        ) AS created_at


    FROM source
)

SELECT * FROM parsed