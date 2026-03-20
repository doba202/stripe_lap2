WITH source AS (
    SELECT
        open_id,
        data_json
    FROM {{ source('stripe_stg', 'stripe_raw_charges') }}
),

parsed AS (
    SELECT
        open_id,
        -- ===== PRIMARY =====
        JSON_VALUE(data_json, '$.id') AS charge_id,

        -- ===== CUSTOMER =====
        JSON_VALUE(data_json, '$.customer') AS customer_id,

        -- ===== PAYMENT INTENT =====
        JSON_VALUE(data_json, '$.payment_intent') AS payment_intent_id,

        -- ===== BALANCE TRANSACTION =====
        JSON_VALUE(data_json, '$.balance_transaction') AS balance_txn_id,
        JSON_VALUE(data_json, '$.failure_balance_transaction') AS failure_balance_txn_id,

        -- ===== INVOICE =====
        JSON_VALUE(data_json, '$.invoice') AS invoice_id,

        -- ===== APP =====
        JSON_VALUE(data_json, '$.application') AS application_id,
        JSON_VALUE(data_json, '$.application_fee') AS application_fee,

        -- ===== AMOUNT =====
        SAFE_CAST(JSON_VALUE(data_json, '$.application_fee_amount') AS INT64) AS application_fee_amount,
        SAFE_CAST(JSON_VALUE(data_json, '$.amount') AS INT64) AS amount,
        SAFE_CAST(JSON_VALUE(data_json, '$.amount_captured') AS INT64) AS amount_captured,
        SAFE_CAST(JSON_VALUE(data_json, '$.amount_refunded') AS INT64) AS amount_refunded,
        SAFE_CAST(JSON_VALUE(data_json, '$.transfer_data.amount') AS INT64) AS transfer_amount,
        JSON_VALUE(data_json, '$.currency') AS currency,

        -- ===== STATUS =====
        JSON_VALUE(data_json, '$.status') AS status,
        CAST(JSON_VALUE(data_json, '$.paid') AS BOOL) AS is_paid,
        CAST(JSON_VALUE(data_json, '$.captured') AS BOOL) AS is_captured,
        CAST(JSON_VALUE(data_json, '$.refunded') AS BOOL) AS is_refunded,
        CAST(JSON_VALUE(data_json, '$.disputed') AS BOOL) AS is_disputed,
        -- ===== STATEMENT DESCRIPTOR (CALCULATED) =====
        JSON_VALUE(data_json, '$.calculated_statement_descriptor') AS calculated_statement_descriptor,
        -- ===== DESCRIPTION =====
        JSON_VALUE(data_json, '$.description') AS description,
        JSON_VALUE(data_json, '$.destination') AS destination,
        JSON_VALUE(data_json, '$.statement_descriptor') AS statement_descriptor,
        JSON_VALUE(data_json, '$.statement_descriptor_suffix') AS statement_descriptor_suffix,

        -- ===== PAYMENT METHOD =====
        JSON_VALUE(data_json, '$.payment_method') AS payment_method_id,
        -- ===== PRESENTMENT (FX / DISPLAY CURRENCY) =====
        SAFE_CAST(JSON_VALUE(data_json, '$.presentment_details.presentment_amount') AS INT64)
            AS presentment_amount,
        JSON_VALUE(data_json, '$.presentment_details.presentment_currency')
            AS presentment_currency,
        -- ===== BILLING =====
        JSON_VALUE(data_json, '$.billing_details.name') AS billing_name,
        JSON_VALUE(data_json, '$.billing_details.email') AS billing_email,
        JSON_VALUE(data_json, '$.billing_details.phone') AS billing_phone,
        JSON_VALUE(data_json, '$.billing_details.tax_id') AS billing_tax_id,

        JSON_VALUE(data_json, '$.billing_details.address.city') AS billing_city,
        JSON_VALUE(data_json, '$.billing_details.address.country') AS billing_country,
        JSON_VALUE(data_json, '$.billing_details.address.line1') AS billing_line1,
        JSON_VALUE(data_json, '$.billing_details.address.line2') AS billing_line2,
        JSON_VALUE(data_json, '$.billing_details.address.postal_code') AS billing_postal_code,

        -- ===== RECEIPT =====
        JSON_VALUE(data_json, '$.receipt_email') AS receipt_email,
        JSON_VALUE(data_json, '$.receipt_number') AS receipt_number,
        JSON_VALUE(data_json, '$.receipt_url') AS receipt_url,

        -- ===== OUTCOME =====
        JSON_VALUE(data_json, '$.outcome.type') AS outcome_type,
        JSON_VALUE(data_json, '$.outcome.reason') AS outcome_reason,

        JSON_VALUE(data_json, '$.outcome.network_status') AS outcome_network_status,
        JSON_VALUE(data_json, '$.outcome.network_advice_code') AS network_advice_code,
        JSON_VALUE(data_json, '$.outcome.network_decline_code') AS network_decline_code,

        JSON_VALUE(data_json, '$.outcome.advice_code') AS outcome_advice_code,

        JSON_VALUE(data_json, '$.outcome.risk_level') AS outcome_risk_level,
        SAFE_CAST(JSON_VALUE(data_json, '$.outcome.risk_score') AS INT64) AS outcome_risk_score,

        JSON_VALUE(data_json, '$.outcome.rule') AS outcome_rule_id,
        JSON_VALUE(data_json, '$.outcome.seller_message') AS outcome_seller_message,

        -- ===== FAILURE =====
        JSON_VALUE(data_json, '$.failure_code') AS failure_code,
        JSON_VALUE(data_json, '$.failure_message') AS failure_message,
        -- ===== REVIEW =====
        JSON_VALUE(data_json, '$.review') AS review_id,

        -- ===== FRAUD =====
        JSON_VALUE(data_json, '$.fraud_details.user_report') AS fraud_user_report,
        JSON_VALUE(data_json, '$.fraud_details.stripe_report') AS fraud_stripe_report,
        -- ===== SHIPPING =====
        JSON_VALUE(data_json, '$.shipping.name') AS shipping_name,
        JSON_VALUE(data_json, '$.shipping.phone') AS shipping_phone,
        JSON_VALUE(data_json, '$.shipping.carrier') AS shipping_carrier,
        JSON_VALUE(data_json, '$.shipping.tracking_number') AS shipping_tracking_number,

        -- ===== SHIPPING ADDRESS =====
        JSON_VALUE(data_json, '$.shipping.address.city') AS shipping_city,
        JSON_VALUE(data_json, '$.shipping.address.country') AS shipping_country,
        JSON_VALUE(data_json, '$.shipping.address.line1') AS shipping_line1,
        JSON_VALUE(data_json, '$.shipping.address.line2') AS shipping_line2,
        JSON_VALUE(data_json, '$.shipping.address.postal_code') AS shipping_postal_code,
        JSON_VALUE(data_json, '$.shipping.address.state') AS shipping_state,
        -- ===== CONNECT =====
        JSON_VALUE(data_json, '$.on_behalf_of') AS on_behalf_of,
        JSON_VALUE(data_json, '$.transfer_data.destination') AS transfer_destination,
        JSON_VALUE(data_json, '$.transfer_group') AS transfer_group,
        JSON_VALUE(data_json, '$.source_transfer') AS source_transfer_id,
        JSON_VALUE(data_json, '$.radar_options.session') AS radar_session_id,
        -- ===== CREATED =====
        TIMESTAMP_SECONDS(
            SAFE_CAST(JSON_VALUE(data_json, '$.created') AS INT64)
        ) AS created_at

    FROM source
)

SELECT * FROM parsed