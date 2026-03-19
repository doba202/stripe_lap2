WITH source AS (
    SELECT
        open_id,
        JSON_VALUE(data_json, '$.id') AS invoice_id,
        JSON_QUERY_ARRAY(data_json, '$.customer_tax_ids') AS tax_ids
    FROM {{ source('stripe_stg', 'stripe_raw_invoices') }}
),

unnested AS (
    SELECT
        open_id,
        invoice_id,
        tax_id
    FROM source,
    UNNEST(tax_ids) AS tax_id
),

parsed AS (
    SELECT
        open_id,
        invoice_id,

        JSON_VALUE(tax_id, '$.type') AS tax_type,
        JSON_VALUE(tax_id, '$.value') AS tax_value

    FROM unnested
)

SELECT * FROM parsed