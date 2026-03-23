WITH source AS (
    SELECT
        open_id,
        data_json
    FROM {{ source('stripe_stg', 'stripe_raw_subscription_items') }}
),

parsed AS (
    SELECT
        open_id,

        -- ===== SCALAR FIELDS (level 1) =====
        JSON_VALUE(data_json, '$.id')               AS id,
        JSON_VALUE(data_json, '$.object')           AS object,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.created') AS INT64)) AS created,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.current_period_end') AS INT64))   AS current_period_end,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.current_period_start') AS INT64)) AS current_period_start,
        SAFE_CAST(JSON_VALUE(data_json, '$.quantity') AS INT64) AS quantity,
        JSON_VALUE(data_json, '$.subscription')     AS subscription,

        -- ===== billing_thresholds (object → flatten) =====
        SAFE_CAST(JSON_VALUE(data_json, '$.billing_thresholds.usage_gte') AS INT64) AS billing_thresholds_usage_gte,

        -- ===== price (object → flatten scalars + recursive objects) =====
        JSON_VALUE(data_json, '$.price.id')                                         AS price_id,
        JSON_VALUE(data_json, '$.price.object')                                     AS price_object,
        CAST(JSON_VALUE(data_json, '$.price.active') AS BOOL)                       AS price_active,
        JSON_VALUE(data_json, '$.price.billing_scheme')                             AS price_billing_scheme,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.price.created') AS INT64)) AS price_created,
        JSON_VALUE(data_json, '$.price.currency')                                   AS price_currency,
        CAST(JSON_VALUE(data_json, '$.price.livemode') AS BOOL)                     AS price_livemode,
        JSON_VALUE(data_json, '$.price.lookup_key')                                 AS price_lookup_key,
        JSON_VALUE(data_json, '$.price.nickname')                                   AS price_nickname,
        JSON_VALUE(data_json, '$.price.product')                                    AS price_product,
        JSON_VALUE(data_json, '$.price.tax_behavior')                               AS price_tax_behavior,
        JSON_VALUE(data_json, '$.price.tiers_mode')                                 AS price_tiers_mode,
        JSON_VALUE(data_json, '$.price.type')                                       AS price_type,
        SAFE_CAST(JSON_VALUE(data_json, '$.price.unit_amount') AS INT64)            AS price_unit_amount,
        JSON_VALUE(data_json, '$.price.unit_amount_decimal')                        AS price_unit_amount_decimal,
        -- price.recurring (nested object → flatten)
        JSON_VALUE(data_json, '$.price.recurring.interval')                         AS price_recurring_interval,
        SAFE_CAST(JSON_VALUE(data_json, '$.price.recurring.interval_count') AS INT64) AS price_recurring_interval_count,
        JSON_VALUE(data_json, '$.price.recurring.meter')                            AS price_recurring_meter,
        JSON_VALUE(data_json, '$.price.recurring.usage_type')                       AS price_recurring_usage_type,
        -- price.custom_unit_amount (nested object → flatten)
        SAFE_CAST(JSON_VALUE(data_json, '$.price.custom_unit_amount.maximum') AS INT64) AS price_custom_unit_amount_maximum,
        SAFE_CAST(JSON_VALUE(data_json, '$.price.custom_unit_amount.minimum') AS INT64) AS price_custom_unit_amount_minimum,
        SAFE_CAST(JSON_VALUE(data_json, '$.price.custom_unit_amount.preset') AS INT64)  AS price_custom_unit_amount_preset,
        -- price.transform_quantity (nested object → flatten)
        SAFE_CAST(JSON_VALUE(data_json, '$.price.transform_quantity.divide_by') AS INT64) AS price_transform_quantity_divide_by,
        JSON_VALUE(data_json, '$.price.transform_quantity.round')                   AS price_transform_quantity_round,
        -- price.metadata — extract specific keys + keep JSON
        JSON_VALUE(data_json, '$.price.metadata.app')                                AS price_metadata_app,
        JSON_VALUE(data_json, '$.price.metadata.platform')                           AS price_metadata_platform,
        JSON_VALUE(data_json, '$.price.metadata.product_id')                         AS price_metadata_product_id,
        JSON_QUERY(data_json, '$.price.metadata')                                    AS price_metadata,
        JSON_QUERY(data_json, '$.price.currency_options')                           AS price_currency_options,
        JSON_QUERY(data_json, '$.price.tiers')                                      AS price_tiers,

        -- ===== ARRAY / COMPLEX FIELDS — giữ nguyên JSON =====
        JSON_QUERY(data_json, '$.discounts')          AS discounts,
        JSON_QUERY(data_json, '$.metadata')           AS metadata,
        JSON_QUERY(data_json, '$.tax_rates')          AS tax_rates

    FROM source
)

SELECT * FROM parsed