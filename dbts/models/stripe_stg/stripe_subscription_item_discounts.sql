WITH source AS (
    SELECT
        open_id,
        JSON_VALUE(data_json, '$.id') AS subscription_item_id,
        JSON_QUERY_ARRAY(data_json, '$.discounts') AS discounts
    FROM {{ source('stripe_stg', 'stripe_raw_subscription_items') }}
),

flatten AS (
    SELECT
        open_id,
        subscription_item_id,
        discount_id
    FROM source,
    UNNEST(discounts) AS discount_id
)

SELECT * FROM flatten