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

taxes AS (
    SELECT
        open_id,
        invoice_id,
        JSON_VALUE(line_item, '$.id') AS line_item_id,
        tax
    FROM line_items,
    UNNEST(JSON_QUERY_ARRAY(line_item, '$.taxes')) AS tax
)

SELECT
    open_id,
    invoice_id,
    line_item_id,

    SAFE_CAST(JSON_VALUE(tax, '$.amount') AS INT64) AS tax_amount,
    JSON_VALUE(tax, '$.tax_behavior') AS tax_behavior,
    JSON_VALUE(tax, '$.taxability_reason') AS taxability_reason,
    JSON_VALUE(tax, '$.tax_rate_details.tax_rate') AS tax_rate_id,
    SAFE_CAST(JSON_VALUE(tax, '$.taxable_amount') AS INT64) AS taxable_amount

FROM taxes