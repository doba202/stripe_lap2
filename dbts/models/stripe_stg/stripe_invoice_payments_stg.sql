WITH source AS (
    SELECT
        JSON_VALUE(data_json, '$.id') AS invoice_id,
        data_json
    FROM {{ source('stripe_stg', 'stripe_raw_invoices') }}
),

payments AS (
    SELECT
        invoice_id,

        JSON_VALUE(payment, '$.id') AS payment_id,

        CAST(JSON_VALUE(payment, '$.amount_paid') AS INT64) AS amount_paid,
        CAST(JSON_VALUE(payment, '$.amount_requested') AS INT64) AS amount_requested,

        JSON_VALUE(payment, '$.currency') AS currency,

        TIMESTAMP_SECONDS(CAST(JSON_VALUE(payment, '$.created') AS INT64)) AS created_at,

        JSON_VALUE(payment, '$.status') AS status,

        -- payment intent
        JSON_VALUE(payment, '$.payment.payment_intent') AS payment_intent_id,

        CAST(JSON_VALUE(payment, '$.livemode') AS BOOL) AS livemode

    FROM source,
    UNNEST(JSON_QUERY_ARRAY(data_json, '$.payments.data')) AS payment
)

SELECT * FROM payments