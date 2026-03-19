WITH source AS (
    SELECT
        open_id,
        JSON_VALUE(data_json, '$.id') AS invoice_id,
        JSON_QUERY_ARRAY(data_json, '$.payments.data') AS payments
    FROM {{ source('stripe_stg', 'stripe_raw_invoices') }}
),

unnested AS (
    SELECT
        open_id,
        invoice_id,
        payment
    FROM source,
    UNNEST(payments) AS payment
),

parsed AS (
    SELECT
        open_id,
        -- ===== KEYS =====
        invoice_id,
        JSON_VALUE(payment, '$.id') AS invoice_payment_id,

        -- ===== AMOUNT =====
        SAFE_CAST(JSON_VALUE(payment, '$.amount_paid') AS INT64) AS amount_paid,
        SAFE_CAST(JSON_VALUE(payment, '$.amount_requested') AS INT64) AS amount_requested,

        JSON_VALUE(payment, '$.currency') AS currency,

        -- ===== STATUS =====
        JSON_VALUE(payment, '$.status') AS status,
        CAST(JSON_VALUE(payment, '$.is_default') AS BOOL) AS is_default,

        -- ===== PAYMENT LINK =====
        JSON_VALUE(payment, '$.invoice') AS invoice_ref_id,

        -- ===== PAYMENT OBJECT =====
        JSON_VALUE(payment, '$.payment.type') AS payment_type,

        JSON_VALUE(payment, '$.payment.payment_intent') AS payment_intent_id,
        JSON_VALUE(payment, '$.payment.charge') AS charge_id,
        JSON_VALUE(payment, '$.payment.payment_record') AS payment_record_id,

        -- ===== TIME =====
        TIMESTAMP_SECONDS(
            SAFE_CAST(JSON_VALUE(payment, '$.created') AS INT64)
        ) AS created_at,

        TIMESTAMP_SECONDS(
            SAFE_CAST(JSON_VALUE(payment, '$.status_transitions.paid_at') AS INT64)
        ) AS paid_at,

        TIMESTAMP_SECONDS(
            SAFE_CAST(JSON_VALUE(payment, '$.status_transitions.canceled_at') AS INT64)
        ) AS canceled_at,

        -- ===== META =====
        CAST(JSON_VALUE(payment, '$.livemode') AS BOOL) AS livemode

    FROM unnested
)

SELECT * FROM parsed