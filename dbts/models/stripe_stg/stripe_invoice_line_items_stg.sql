WITH source AS (
    SELECT
        open_id,
        data_json
    FROM {{ source('stripe_stg', 'stripe_raw_invoices') }}
),

line_items AS (
    SELECT
        open_id,
        JSON_VALUE(data_json, '$.id') AS invoice_id,
        line_item
    FROM source,
    UNNEST(JSON_QUERY_ARRAY(data_json, '$.lines.data')) AS line_item
)

SELECT
    open_id,
    -- ===== KEYS =====
    invoice_id,
    JSON_VALUE(line_item, '$.id') AS line_item_id,

    -- ===== BASIC =====
    JSON_VALUE(line_item, '$.description') AS description,
    JSON_VALUE(line_item, '$.currency') AS currency,

    SAFE_CAST(JSON_VALUE(line_item, '$.amount') AS INT64) AS amount,
    SAFE_CAST(JSON_VALUE(line_item, '$.quantity') AS INT64) AS quantity,
    SAFE_CAST(JSON_VALUE(line_item, '$.subtotal') AS INT64) AS subtotal,

    -- ===== PERIOD =====
    TIMESTAMP_SECONDS(
        SAFE_CAST(JSON_VALUE(line_item, '$.period.start') AS INT64)
    ) AS period_start,

    TIMESTAMP_SECONDS(
        SAFE_CAST(JSON_VALUE(line_item, '$.period.end') AS INT64)
    ) AS period_end,

    -- ===== PRICING =====
    JSON_VALUE(line_item, '$.pricing.type') AS pricing_type,
    JSON_VALUE(line_item, '$.pricing.price_details.price') AS price_id,
    JSON_VALUE(line_item, '$.pricing.price_details.product') AS product_id,

    SAFE_CAST(JSON_VALUE(line_item, '$.pricing.unit_amount_decimal') AS NUMERIC)
        AS unit_amount_decimal,

    -- ===== PARENT =====
    JSON_VALUE(line_item, '$.parent.type') AS parent_type,

    JSON_VALUE(line_item, '$.parent.invoice_item_details.invoice_item')
        AS invoice_item_id,

    CAST(JSON_VALUE(line_item, '$.parent.invoice_item_details.proration') AS BOOL)
        AS is_proration,

    JSON_VALUE(line_item, '$.parent.invoice_item_details.subscription')
        AS subscription_id,

    JSON_VALUE(line_item, '$.parent.subscription_item_details.subscription_item')
        AS subscription_item_id,

    -- ===== FLAGS =====
    CAST(JSON_VALUE(line_item, '$.discountable') AS BOOL) AS is_discountable

FROM line_items