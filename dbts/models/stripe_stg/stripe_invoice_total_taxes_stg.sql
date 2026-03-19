WITH source AS (
    SELECT
        JSON_VALUE(data_json, '$.id') AS invoice_id,
        JSON_QUERY_ARRAY(data_json, '$.total_taxes') AS total_taxes
    FROM {{ source('stripe_stg', 'stripe_raw_invoices') }}
),

unnested AS (
    SELECT
        invoice_id,
        tax
    FROM source,
    UNNEST(total_taxes) AS tax
),

parsed AS (
    SELECT
        -- ===== FK =====
        invoice_id,

        -- ===== AMOUNT =====
        SAFE_CAST(JSON_VALUE(tax, '$.amount') AS INT64) AS amount,
        SAFE_CAST(JSON_VALUE(tax, '$.taxable_amount') AS INT64) AS taxable_amount,

        -- ===== TYPE =====
        JSON_VALUE(tax, '$.type') AS tax_type,
        JSON_VALUE(tax, '$.tax_behavior') AS tax_behavior,

        -- ===== TAX RATE =====
        JSON_VALUE(tax, '$.tax_rate_details.tax_rate') AS tax_rate_id,

        -- ===== REASON =====
        JSON_VALUE(tax, '$.taxability_reason') AS taxability_reason

    FROM unnested
)

SELECT * FROM parsed