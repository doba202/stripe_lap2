WITH source AS (
    SELECT
        id AS raw_id,
        data_json
    FROM {{ source('stripe_stg', 'stripe_raw_charges') }}
),

parsed AS (
    SELECT
        -- ===== PRIMARY =====
        JSON_VALUE(data_json, '$.id') AS charge_id,

        -- ===== CUSTOMER =====
        JSON_VALUE(data_json, '$.customer') AS customer_id,

        -- ===== PAYMENT INTENT =====
        JSON_VALUE(data_json, '$.payment_intent') AS payment_intent_id,

        -- ===== BALANCE TRANSACTION =====
        JSON_VALUE(data_json, '$.balance_transaction') AS balance_txn_id,

        -- ===== INVOICE =====
        JSON_VALUE(data_json, '$.invoice') AS invoice_id,

        -- ===== AMOUNT =====
        SAFE_CAST(JSON_VALUE(data_json, '$.amount') AS INT64) AS amount,
        SAFE_CAST(JSON_VALUE(data_json, '$.amount_captured') AS INT64) AS amount_captured,
        SAFE_CAST(JSON_VALUE(data_json, '$.amount_refunded') AS INT64) AS amount_refunded,
        SAFE_CAST(JSON_VALUE(data_json, '$.application_fee_amount') AS INT64) AS application_fee_amount,

        JSON_VALUE(data_json, '$.currency') AS currency,

        -- ===== STATUS =====
        JSON_VALUE(data_json, '$.status') AS status,
        CAST(JSON_VALUE(data_json, '$.paid') AS BOOL) AS is_paid,
        CAST(JSON_VALUE(data_json, '$.captured') AS BOOL) AS is_captured,
        CAST(JSON_VALUE(data_json, '$.refunded') AS BOOL) AS is_refunded,
        CAST(JSON_VALUE(data_json, '$.disputed') AS BOOL) AS is_disputed,

        -- ===== DESCRIPTION =====
        JSON_VALUE(data_json, '$.description') AS description,
        JSON_VALUE(data_json, '$.statement_descriptor') AS statement_descriptor,
        JSON_VALUE(data_json, '$.statement_descriptor_suffix') AS statement_descriptor_suffix,

        -- ===== PAYMENT METHOD =====
        JSON_VALUE(data_json, '$.payment_method') AS payment_method_id,
        JSON_VALUE(data_json, '$.payment_method_details.type') AS payment_method_type,

        -- ===== CARD INFO =====
        JSON_VALUE(data_json, '$.payment_method_details.card.brand') AS card_brand,
        JSON_VALUE(data_json, '$.payment_method_details.card.funding') AS card_funding,
        JSON_VALUE(data_json, '$.payment_method_details.card.country') AS card_country,
        JSON_VALUE(data_json, '$.payment_method_details.card.network') AS card_network,

        -- ===== CARD CHECKS =====
        JSON_VALUE(data_json, '$.payment_method_details.card.checks.cvc_check') AS card_cvc_check,
        JSON_VALUE(data_json, '$.payment_method_details.card.checks.address_postal_code_check')
            AS card_postal_check,

        -- ===== BILLING =====
        JSON_VALUE(data_json, '$.billing_details.name') AS billing_name,
        JSON_VALUE(data_json, '$.billing_details.email') AS billing_email,
        JSON_VALUE(data_json, '$.billing_details.phone') AS billing_phone,

        JSON_VALUE(data_json, '$.billing_details.address.city') AS billing_city,
        JSON_VALUE(data_json, '$.billing_details.address.country') AS billing_country,
        JSON_VALUE(data_json, '$.billing_details.address.line1') AS billing_line1,
        JSON_VALUE(data_json, '$.billing_details.address.postal_code') AS billing_postal_code,

        -- ===== RECEIPT =====
        JSON_VALUE(data_json, '$.receipt_email') AS receipt_email,
        JSON_VALUE(data_json, '$.receipt_number') AS receipt_number,
        JSON_VALUE(data_json, '$.receipt_url') AS receipt_url,

        -- ===== OUTCOME =====
        JSON_VALUE(data_json, '$.outcome.network_status') AS network_status,
        JSON_VALUE(data_json, '$.outcome.risk_level') AS risk_level,
        JSON_VALUE(data_json, '$.outcome.risk_score') AS risk_score,
        JSON_VALUE(data_json, '$.outcome.seller_message') AS seller_message,
        JSON_VALUE(data_json, '$.outcome.type') AS outcome_type,

        -- ===== FAILURE =====
        JSON_VALUE(data_json, '$.failure_code') AS failure_code,
        JSON_VALUE(data_json, '$.failure_message') AS failure_message,

        -- ===== FRAUD =====
        JSON_VALUE(data_json, '$.fraud_details.user_report') AS fraud_user_report,
        JSON_VALUE(data_json, '$.fraud_details.stripe_report') AS fraud_stripe_report,

        -- ===== REFUND INFO =====
        JSON_VALUE(data_json, '$.refunds.url') AS refunds_url,
        CAST(JSON_VALUE(data_json, '$.refunds.has_more') AS BOOL) AS refunds_has_more,

        -- ===== CONNECT =====
        JSON_VALUE(data_json, '$.on_behalf_of') AS on_behalf_of,
        JSON_VALUE(data_json, '$.transfer_data.destination') AS transfer_destination,
        JSON_VALUE(data_json, '$.transfer_group') AS transfer_group,

        -- ===== CREATED =====
        TIMESTAMP_SECONDS(
            SAFE_CAST(JSON_VALUE(data_json, '$.created') AS INT64)
        ) AS created_at,

        raw_id

    FROM source
)

SELECT * FROM parsed