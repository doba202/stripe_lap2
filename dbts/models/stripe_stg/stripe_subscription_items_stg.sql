WITH source AS (
    SELECT
        data_json
    FROM {{ source('stripe_stg', 'stripe_raw_subscription_items') }}
),

parsed AS (
    SELECT
        -- ===== PRIMARY =====
        JSON_VALUE(data_json, '$.id') AS subscription_item_id,

        -- ===== LINK =====
        JSON_VALUE(data_json, '$.subscription') AS subscription_id,

        -- ===== QUANTITY =====
        SAFE_CAST(JSON_VALUE(data_json, '$.quantity') AS INT64) AS quantity,

        -- ===== TIME =====
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.created') AS INT64))
            AS created_at,

        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.current_period_start') AS INT64))
            AS current_period_start,

        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.current_period_end') AS INT64))
            AS current_period_end,

        -- ===== PLAN =====
        JSON_VALUE(data_json, '$.plan.id') AS plan_id,
        CAST(JSON_VALUE(data_json, '$.plan.active') AS BOOL) AS plan_active,
        SAFE_CAST(JSON_VALUE(data_json, '$.plan.amount') AS INT64) AS plan_amount,
        JSON_VALUE(data_json, '$.plan.amount_decimal') AS plan_amount_decimal,
        JSON_VALUE(data_json, '$.plan.billing_scheme') AS plan_billing_scheme,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.plan.created') AS INT64))
            AS plan_created_at,
        JSON_VALUE(data_json, '$.plan.currency') AS plan_currency,
        JSON_VALUE(data_json, '$.plan.interval') AS plan_interval,
        SAFE_CAST(JSON_VALUE(data_json, '$.plan.interval_count') AS INT64)
            AS plan_interval_count,
        JSON_VALUE(data_json, '$.plan.nickname') AS plan_nickname,
        JSON_VALUE(data_json, '$.plan.product') AS product_id,
        JSON_VALUE(data_json, '$.plan.tiers_mode') AS plan_tiers_mode,
        JSON_VALUE(data_json, '$.plan.usage_type') AS plan_usage_type,
        SAFE_CAST(JSON_VALUE(data_json, '$.plan.trial_period_days') AS INT64)
            AS plan_trial_period_days,

        -- ===== PRICE =====
        JSON_VALUE(data_json, '$.price.id') AS price_id,
        CAST(JSON_VALUE(data_json, '$.price.active') AS BOOL) AS price_active,
        JSON_VALUE(data_json, '$.price.billing_scheme') AS price_billing_scheme,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.price.created') AS INT64))
            AS price_created_at,
        JSON_VALUE(data_json, '$.price.currency') AS price_currency,
        JSON_VALUE(data_json, '$.price.lookup_key') AS price_lookup_key,
        JSON_VALUE(data_json, '$.price.nickname') AS price_nickname,
        JSON_VALUE(data_json, '$.price.product') AS price_product_id,
        JSON_VALUE(data_json, '$.price.tax_behavior') AS price_tax_behavior,
        JSON_VALUE(data_json, '$.price.type') AS price_type,

        SAFE_CAST(JSON_VALUE(data_json, '$.price.unit_amount') AS INT64)
            AS price_unit_amount,
        JSON_VALUE(data_json, '$.price.unit_amount_decimal') AS price_unit_amount_decimal,

        -- ===== PRICE RECURRING =====
        JSON_VALUE(data_json, '$.price.recurring.interval') AS price_interval,
        SAFE_CAST(JSON_VALUE(data_json, '$.price.recurring.interval_count') AS INT64)
            AS price_interval_count,
        SAFE_CAST(JSON_VALUE(data_json, '$.price.recurring.trial_period_days') AS INT64)
            AS price_trial_period_days,
        JSON_VALUE(data_json, '$.price.recurring.usage_type') AS price_usage_type

    FROM source
)

SELECT * FROM parsed