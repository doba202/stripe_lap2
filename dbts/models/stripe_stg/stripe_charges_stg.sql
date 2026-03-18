WITH source AS (
    SELECT
        id AS raw_id,
        data_json
    FROM {{ source('stripe_stg', 'stripe_raw_charges') }}
),

parsed AS (
    SELECT
        -- ===== PRIMARY KEY =====
        JSON_VALUE(data_json, '$.id') AS charge_id,

        -- ===== CUSTOMER =====
        JSON_VALUE(data_json, '$.customer') AS customer_id,

        -- ===== PAYMENT INTENT (LINK 🔥) =====
        JSON_VALUE(data_json, '$.payment_intent') AS payment_intent_id,

        -- ===== BALANCE TRANSACTION (SUPER IMPORTANT 💰) =====
        JSON_VALUE(data_json, '$.balance_transaction') AS balance_txn_id,

        -- ===== AMOUNT =====
        SAFE_CAST(JSON_VALUE(data_json, '$.amount') AS INT64) AS amount,
        SAFE_CAST(JSON_VALUE(data_json, '$.amount_captured') AS INT64) AS amount_captured,
        SAFE_CAST(JSON_VALUE(data_json, '$.amount_refunded') AS INT64) AS amount_refunded,
        JSON_VALUE(data_json, '$.currency') AS currency,

        -- ===== STATUS =====
        JSON_VALUE(data_json, '$.status') AS status,
        CAST(JSON_VALUE(data_json, '$.paid') AS BOOL) AS is_paid,
        CAST(JSON_VALUE(data_json, '$.captured') AS BOOL) AS is_captured,
        CAST(JSON_VALUE(data_json, '$.refunded') AS BOOL) AS is_refunded,

        -- ===== DESCRIPTION =====
        JSON_VALUE(data_json, '$.description') AS description,

        -- ===== PAYMENT METHOD =====
        JSON_VALUE(data_json, '$.payment_method') AS payment_method_id,
        JSON_VALUE(data_json, '$.payment_method_details.type') AS payment_method_type,

        -- ===== CARD INFO (useful for analytics) =====
        JSON_VALUE(data_json, '$.payment_method_details.card.brand') AS card_brand,
        JSON_VALUE(data_json, '$.payment_method_details.card.funding') AS card_funding,
        JSON_VALUE(data_json, '$.payment_method_details.card.country') AS card_country,

        -- ===== BILLING =====
        JSON_VALUE(data_json, '$.billing_details.address.country') AS billing_country,
        JSON_VALUE(data_json, '$.billing_details.email') AS billing_email,

        -- ===== RECEIPT =====
        JSON_VALUE(data_json, '$.receipt_email') AS receipt_email,
        JSON_VALUE(data_json, '$.receipt_url') AS receipt_url,

        -- ===== OUTCOME =====
        JSON_VALUE(data_json, '$.outcome.network_status') AS network_status,
        JSON_VALUE(data_json, '$.outcome.risk_level') AS risk_level,

        -- ===== CREATED =====
        TIMESTAMP_SECONDS(
            SAFE_CAST(JSON_VALUE(data_json, '$.created') AS INT64)
        ) AS created_at,

        raw_id

    FROM source
)

SELECT * FROM parsed