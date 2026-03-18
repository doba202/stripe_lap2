WITH source AS (
    SELECT
        id AS raw_id,
        call_at,
        data_json
    FROM {{ source('stripe_stg', 'stripe_raw_invoices') }}
),

parsed AS (
    SELECT
        -- ===== PRIMARY KEY =====
        JSON_VALUE(data_json, '$.id') AS invoice_id,

        -- ===== BASIC =====
        JSON_VALUE(data_json, '$.status') AS status,
        JSON_VALUE(data_json, '$.currency') AS currency,
        JSON_VALUE(data_json, '$.description') AS description,

        -- ===== CUSTOMER =====
        JSON_VALUE(data_json, '$.customer') AS customer_id,
        JSON_VALUE(data_json, '$.customer_email') AS customer_email,

        -- ===== AMOUNT =====
        CAST(JSON_VALUE(data_json, '$.amount_due') AS INT64) AS amount_due,
        CAST(JSON_VALUE(data_json, '$.amount_paid') AS INT64) AS amount_paid,
        CAST(JSON_VALUE(data_json, '$.amount_remaining') AS INT64) AS amount_remaining,
        CAST(JSON_VALUE(data_json, '$.total') AS INT64) AS total,

        -- ===== BILLING =====
        JSON_VALUE(data_json, '$.billing_reason') AS billing_reason,
        JSON_VALUE(data_json, '$.collection_method') AS collection_method,
        CAST(JSON_VALUE(data_json, '$.auto_advance') AS BOOL) AS auto_advance,

        -- ===== TIME =====
        TIMESTAMP_SECONDS(CAST(JSON_VALUE(data_json, '$.created') AS INT64)) AS created_at,
        TIMESTAMP_SECONDS(CAST(JSON_VALUE(data_json, '$.period_start') AS INT64)) AS period_start,
        TIMESTAMP_SECONDS(CAST(JSON_VALUE(data_json, '$.period_end') AS INT64)) AS period_end,

        -- ===== STATUS TRANSITIONS =====
        TIMESTAMP_SECONDS(CAST(JSON_VALUE(data_json, '$.status_transitions.paid_at') AS INT64)) AS paid_at,
        TIMESTAMP_SECONDS(CAST(JSON_VALUE(data_json, '$.status_transitions.finalized_at') AS INT64)) AS finalized_at,

        -- ===== HOSTED =====
        JSON_VALUE(data_json, '$.hosted_invoice_url') AS hosted_invoice_url,

        -- ===== SUBSCRIPTION =====
        JSON_VALUE(data_json, '$.parent.subscription_details.subscription') AS subscription_id,

        -- ===== TAX =====
        CAST(JSON_VALUE(data_json, '$.automatic_tax.enabled') AS BOOL) AS automatic_tax_enabled,
        JSON_VALUE(data_json, '$.automatic_tax.status') AS automatic_tax_status,

        -- ===== FLAGS =====
        CAST(JSON_VALUE(data_json, '$.livemode') AS BOOL) AS livemode,
        call_at

    FROM source
)

SELECT * FROM parsed