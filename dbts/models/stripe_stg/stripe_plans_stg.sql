WITH source AS (
    SELECT
        open_id,
        data_json
    FROM {{ source('stripe_stg', 'stripe_raw_plans') }}
),

parsed AS (
    SELECT
        open_id,

        -- ===== SCALAR FIELDS (level 1) =====
        JSON_VALUE(data_json, '$.id')                   AS id,
        JSON_VALUE(data_json, '$.object')               AS object,
        CAST(JSON_VALUE(data_json, '$.active') AS BOOL) AS active,
        SAFE_CAST(JSON_VALUE(data_json, '$.amount') AS INT64)    AS amount,
        JSON_VALUE(data_json, '$.amount_decimal')       AS amount_decimal,
        JSON_VALUE(data_json, '$.billing_scheme')       AS billing_scheme,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.created') AS INT64)) AS created,
        JSON_VALUE(data_json, '$.currency')             AS currency,
        JSON_VALUE(data_json, '$.interval')             AS `interval`,
        SAFE_CAST(JSON_VALUE(data_json, '$.interval_count') AS INT64) AS interval_count,
        CAST(JSON_VALUE(data_json, '$.livemode') AS BOOL)        AS livemode,
        JSON_VALUE(data_json, '$.meter')                AS meter,
        JSON_VALUE(data_json, '$.nickname')             AS nickname,
        JSON_VALUE(data_json, '$.product')              AS product,
        JSON_VALUE(data_json, '$.tiers_mode')           AS tiers_mode,
        SAFE_CAST(JSON_VALUE(data_json, '$.trial_period_days') AS INT64) AS trial_period_days,
        JSON_VALUE(data_json, '$.usage_type')           AS usage_type,

        -- ===== transform_usage (object → flatten) =====
        SAFE_CAST(JSON_VALUE(data_json, '$.transform_usage.divide_by') AS INT64) AS transform_usage_divide_by,
        JSON_VALUE(data_json, '$.transform_usage.round')                         AS transform_usage_round,

        -- ===== ARRAY / COMPLEX FIELDS — giữ nguyên JSON =====
        JSON_QUERY(data_json, '$.metadata')             AS metadata,
        JSON_QUERY(data_json, '$.tiers')                AS tiers

    FROM source
)

SELECT * FROM parsed