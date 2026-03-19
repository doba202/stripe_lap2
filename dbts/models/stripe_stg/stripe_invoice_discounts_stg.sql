WITH source AS (
    SELECT
        JSON_VALUE(data_json, '$.id') AS invoice_id,
        JSON_QUERY_ARRAY(data_json, '$.discounts') AS discounts
    FROM {{ source('stripe_stg', 'stripe_raw_invoices') }}
),

unnested AS (
    SELECT
        invoice_id,
        discount
    FROM source,
    UNNEST(discounts) AS discount
),

parsed AS (
    SELECT
        invoice_id,

        -- vì nó là string JSON → phải extract
        JSON_VALUE(discount, '$') AS discount_id

    FROM unnested
)

SELECT * FROM parsed