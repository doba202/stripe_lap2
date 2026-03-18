WITH source AS (
    SELECT
        id AS raw_id,
        data_json
    FROM {{ source('stripe_stg', 'stripe_raw_payment_intents') }}
),

parsed AS (
    SELECT
        -- ===== PRIMARY KEY =====
        JSON_VALUE(data_json, '$.id') AS payment_intent_id,

        -- ===== CUSTOMER =====
        JSON_VALUE(data_json, '$.customer') AS customer_id,

        -- ===== AMOUNT =====
        SAFE_CAST(JSON_VALUE(data_json, '$.amount') AS INT64) AS amount,
        SAFE_CAST(JSON_VALUE(data_json, '$.amount_received') AS INT64) AS amount_received,
        SAFE_CAST(JSON_VALUE(data_json, '$.amount_capturable') AS INT64) AS amount_capturable,
        JSON_VALUE(data_json, '$.currency') AS currency,

        -- ===== STATUS =====
        JSON_VALUE(data_json, '$.status') AS status,

        -- ===== PAYMENT METHOD =====
        JSON_VALUE(data_json, '$.payment_method') AS payment_method_id,

        -- ===== CHARGE =====
        JSON_VALUE(data_json, '$.latest_charge') AS charge_id,

        -- ===== INVOICE LINK (IMPORTANT 🔥) =====
        JSON_VALUE(data_json, '$.payment_details.order_reference') AS invoice_id,

        -- ===== DESCRIPTION =====
        JSON_VALUE(data_json, '$.description') AS description,

        -- ===== RECEIPT =====
        JSON_VALUE(data_json, '$.receipt_email') AS receipt_email,

        -- ===== CREATED =====
        TIMESTAMP_SECONDS(
            SAFE_CAST(JSON_VALUE(data_json, '$.created') AS INT64)
        ) AS created_at,

        raw_id

    FROM source
)

SELECT * FROM parsed