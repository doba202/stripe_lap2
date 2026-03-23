WITH source AS (
    SELECT
        open_id,
        data_json
    FROM {{ source('stripe_stg', 'stripe_raw_subscription_items') }}
),

parsed AS (
    SELECT
        open_id,
        -- ===== PRIMARY =====
        JSON_VALUE(data_json, '$.id') AS subscription_item_id,
        JSON_VALUE(data_json, '$.subscription') AS subscription_id,

        -- ===== QUANTITY =====
        SAFE_CAST(JSON_VALUE(data_json, '$.quantity') AS INT64) AS quantity,

        -- ===== BILLING THRESHOLDS =====
        SAFE_CAST(JSON_VALUE(data_json, '$.billing_thresholds.usage_gte') AS INT64)
            AS billing_threshold_usage_gte,

        -- ===== PERIOD =====
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.current_period_start') AS INT64))
            AS current_period_start,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.current_period_end') AS INT64))
            AS current_period_end,

        -- ===== PRICE (flatten key fields — chi tiết có thể có bảng prices riêng) =====
        JSON_VALUE(data_json, '$.price.id') AS price_id,
        JSON_VALUE(data_json, '$.price.product') AS product_id,
        CAST(JSON_VALUE(data_json, '$.price.active') AS BOOL) AS price_active,
        JSON_VALUE(data_json, '$.price.type') AS price_type,
        JSON_VALUE(data_json, '$.price.billing_scheme') AS price_billing_scheme,
        JSON_VALUE(data_json, '$.price.currency') AS price_currency,
        SAFE_CAST(JSON_VALUE(data_json, '$.price.unit_amount') AS INT64) AS price_unit_amount,
        JSON_VALUE(data_json, '$.price.unit_amount_decimal') AS price_unit_amount_decimal,
        JSON_VALUE(data_json, '$.price.nickname') AS price_nickname,
        JSON_VALUE(data_json, '$.price.lookup_key') AS price_lookup_key,
        JSON_VALUE(data_json, '$.price.tax_behavior') AS price_tax_behavior,
        JSON_VALUE(data_json, '$.price.tiers_mode') AS price_tiers_mode,

        -- price.recurring
        JSON_VALUE(data_json, '$.price.recurring.interval') AS price_interval,
        SAFE_CAST(JSON_VALUE(data_json, '$.price.recurring.interval_count') AS INT64)
            AS price_interval_count,
        JSON_VALUE(data_json, '$.price.recurring.usage_type') AS price_usage_type,
        JSON_VALUE(data_json, '$.price.recurring.meter') AS price_meter,

        -- price.custom_unit_amount
        SAFE_CAST(JSON_VALUE(data_json, '$.price.custom_unit_amount.minimum') AS INT64)
            AS price_custom_unit_min,
        SAFE_CAST(JSON_VALUE(data_json, '$.price.custom_unit_amount.maximum') AS INT64)
            AS price_custom_unit_max,
        SAFE_CAST(JSON_VALUE(data_json, '$.price.custom_unit_amount.preset') AS INT64)
            AS price_custom_unit_preset,

        -- price.transform_quantity
        SAFE_CAST(JSON_VALUE(data_json, '$.price.transform_quantity.divide_by') AS INT64)
            AS price_transform_divide_by,
        JSON_VALUE(data_json, '$.price.transform_quantity.round') AS price_transform_round,

        -- ===== DISCOUNTS (discount ids) =====
        ARRAY(
            SELECT JSON_VALUE(d)
            FROM UNNEST(JSON_QUERY_ARRAY(data_json, '$.discounts')) AS d
        ) AS discount_ids,

        -- ===== TAX RATES (chỉ lưu id — chi tiết join từ bảng tax_rates) =====
        ARRAY(
            SELECT AS STRUCT
                JSON_VALUE(tr, '$.id') AS tax_rate_id
            FROM UNNEST(JSON_QUERY_ARRAY(data_json, '$.tax_rates')) AS tr
        ) AS tax_rate_ids,

        -- ===== METADATA =====
        JSON_QUERY(data_json, '$.metadata') AS metadata_json,

        -- ===== CREATED =====
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.created') AS INT64))
            AS created_at

    FROM source
)

SELECT * FROM parsed