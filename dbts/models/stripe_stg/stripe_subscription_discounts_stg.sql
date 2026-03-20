WITH source AS (
    SELECT
    open_id,
        JSON_VALUE(data_json, '$.id') AS subscription_id,
        data_json
    FROM {{ source('stripe_stg', 'stripe_raw_subscriptions') }}
),

unnested AS (
    SELECT
    open_id,
        subscription_id,
        discount_id
    FROM source,
    UNNEST(JSON_QUERY_ARRAY(data_json, '$.discounts')) AS discount_id
)

SELECT
open_id,
    subscription_id,
    JSON_VALUE(discount_id) AS discount_id
FROM unnested