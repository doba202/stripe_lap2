WITH source AS (
    SELECT
        JSON_VALUE(data_json, '$.id') AS invoice_id,
        JSON_QUERY_ARRAY(data_json, '$.default_tax_rates') AS tax_rates
    FROM {{ source('stripe_stg', 'stripe_raw_invoices') }}
),

unnested AS (
    SELECT
        invoice_id,
        tax_rate
    FROM source,
    UNNEST(tax_rates) AS tax_rate
),

parsed AS (
    SELECT
        -- ===== FK =====
        invoice_id,

        -- ===== PRIMARY =====
        JSON_VALUE(tax_rate, '$.id') AS tax_rate_id,

        -- ===== BASIC =====
        JSON_VALUE(tax_rate, '$.object') AS object,
        CAST(JSON_VALUE(tax_rate, '$.active') AS BOOL) AS is_active,

        -- ===== LOCATION =====
        JSON_VALUE(tax_rate, '$.country') AS country,
        JSON_VALUE(tax_rate, '$.state') AS state,
        JSON_VALUE(tax_rate, '$.jurisdiction') AS jurisdiction,
        JSON_VALUE(tax_rate, '$.jurisdiction_level') AS jurisdiction_level,

        -- ===== DISPLAY =====
        JSON_VALUE(tax_rate, '$.display_name') AS display_name,
        JSON_VALUE(tax_rate, '$.description') AS description,

        -- ===== RATE =====
        SAFE_CAST(JSON_VALUE(tax_rate, '$.percentage') AS FLOAT64) AS percentage,
        SAFE_CAST(JSON_VALUE(tax_rate, '$.effective_percentage') AS FLOAT64) AS effective_percentage,
        JSON_VALUE(tax_rate, '$.rate_type') AS rate_type,
        JSON_VALUE(tax_rate, '$.tax_type') AS tax_type,

        CAST(JSON_VALUE(tax_rate, '$.inclusive') AS BOOL) AS is_inclusive,

        -- ===== FLAT AMOUNT (nested object) =====
        SAFE_CAST(JSON_VALUE(tax_rate, '$.flat_amount.amount') AS INT64) AS flat_amount,
        JSON_VALUE(tax_rate, '$.flat_amount.currency') AS flat_amount_currency,

        -- ===== TIME =====
        TIMESTAMP_SECONDS(
            SAFE_CAST(JSON_VALUE(tax_rate, '$.created') AS INT64)
        ) AS created_at,

        -- ===== META =====
        CAST(JSON_VALUE(tax_rate, '$.livemode') AS BOOL) AS livemode,
        JSON_QUERY(tax_rate, '$.metadata') AS metadata_json

    FROM unnested
)

SELECT * FROM parsed