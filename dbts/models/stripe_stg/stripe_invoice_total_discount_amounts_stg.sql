WITH source AS (
    SELECT
        open_id,
        JSON_VALUE(data_json, '$.id') AS invoice_id,
        JSON_QUERY_ARRAY(data_json, '$.total_discount_amounts') AS discount_amounts
    FROM {{ source('stripe_stg', 'stripe_raw_invoices') }}
),

unnested AS (
    SELECT
        open_id,
        invoice_id,
        discount_amount
    FROM source,
    UNNEST(discount_amounts) AS discount_amount
),

parsed AS (
    SELECT
        open_id,
        -- ===== FK =====
        invoice_id,

        -- ===== VALUE =====
        SAFE_CAST(JSON_VALUE(discount_amount, '$.amount') AS INT64) AS amount,

        -- ===== DISCOUNT =====
        JSON_VALUE(discount_amount, '$.discount') AS discount_id

    FROM unnested
)

SELECT * FROM parsed