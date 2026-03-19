WITH source AS (
    SELECT open_id,data_json
    FROM {{ source('stripe_stg', 'stripe_raw_invoices') }}
),

line_items AS (
    SELECT
        open_id,
        JSON_VALUE(data_json, '$.id') AS invoice_id,
        line_item
    FROM source,
    UNNEST(JSON_QUERY_ARRAY(data_json, '$.lines.data')) AS line_item
),

discounts AS (
    SELECT
        open_id,
        invoice_id,
        JSON_VALUE(line_item, '$.id') AS line_item_id,
        discount_id
    FROM line_items,
    UNNEST(JSON_VALUE_ARRAY(line_item, '$.discounts')) AS discount_id
)

SELECT
    open_id,
    invoice_id,
    line_item_id,
    discount_id
FROM discounts