WITH source AS (
    SELECT
        open_id,
        JSON_VALUE(data_json, '$.id') AS invoice_id,
        JSON_QUERY_ARRAY(data_json, '$.discounts') AS discounts
    FROM {{ source('stripe_stg', 'stripe_raw_invoices') }}
),

unnested AS (
    SELECT
        open_id,
        invoice_id,
        discount
    FROM source,
    UNNEST(discounts) AS discount
),

parsed AS (
    SELECT
        open_id,
        invoice_id,

        -- vì nó là string JSON → phải extract
        JSON_VALUE(discount, '$') AS discount_id

    FROM unnested
)

SELECT * FROM parsed