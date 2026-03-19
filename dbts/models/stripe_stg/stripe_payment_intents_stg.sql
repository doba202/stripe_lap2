WITH source AS (
    SELECT
        data_json
    FROM {{ source('stripe_stg', 'stripe_raw_payment_intents') }}
),

parsed AS (
    SELECT
        -- ===== PRIMARY =====
        JSON_VALUE(data_json, '$.id') AS payment_intent_id,

        -- ===== AMOUNT =====
        SAFE_CAST(JSON_VALUE(data_json, '$.amount') AS INT64) AS amount,
        SAFE_CAST(JSON_VALUE(data_json, '$.amount_received') AS INT64) AS amount_received,
        SAFE_CAST(JSON_VALUE(data_json, '$.amount_capturable') AS INT64) AS amount_capturable,
        JSON_VALUE(data_json, '$.currency') AS currency,

        -- ===== STATUS =====
        JSON_VALUE(data_json, '$.status') AS status,

        -- ===== CUSTOMER =====
        JSON_VALUE(data_json, '$.customer') AS customer_id,

        -- ===== PAYMENT METHOD =====
        JSON_VALUE(data_json, '$.payment_method') AS payment_method_id,

        -- ===== INVOICE / SETUP =====
        JSON_VALUE(data_json, '$.invoice') AS invoice_id,
        JSON_VALUE(data_json, '$.setup_future_usage') AS setup_future_usage,

        -- ===== DESCRIPTION =====
        JSON_VALUE(data_json, '$.description') AS description,
        JSON_VALUE(data_json, '$.receipt_email') AS receipt_email,

        -- ===== CONFIG =====
        JSON_VALUE(data_json, '$.capture_method') AS capture_method,
        JSON_VALUE(data_json, '$.confirmation_method') AS confirmation_method,

        -- ===== AUTO PAYMENT =====
        CAST(JSON_VALUE(data_json, '$.automatic_payment_methods.enabled') AS BOOL)
            AS automatic_payment_methods_enabled,

        -- ===== REVIEW / FRAUD =====
        JSON_VALUE(data_json, '$.review') AS review_id,

        -- ===== STATEMENT =====
        JSON_VALUE(data_json, '$.statement_descriptor') AS statement_descriptor,
        JSON_VALUE(data_json, '$.statement_descriptor_suffix') AS statement_descriptor_suffix,

        -- ===== CANCELLATION =====
        JSON_VALUE(data_json, '$.cancellation_reason') AS cancellation_reason,

        -- ===== LINKS =====
        JSON_VALUE(data_json, '$.latest_charge') AS latest_charge_id,

        -- ===== CONNECT =====
        JSON_VALUE(data_json, '$.on_behalf_of') AS on_behalf_of,
        JSON_VALUE(data_json, '$.transfer_group') AS transfer_group,

        -- ===== NEXT ACTION =====
        JSON_VALUE(data_json, '$.next_action.type') AS next_action_type,
        JSON_VALUE(data_json, '$.next_action.redirect_to_url.url') AS next_action_redirect_url,
        JSON_VALUE(data_json, '$.next_action.redirect_to_url.return_url') AS next_action_return_url,

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

        -- ===== PROCESSING =====
        JSON_VALUE(data_json, '$.processing.type') AS processing_type,
        JSON_VALUE(data_json, '$.processing.card.customer_notification')
            AS processing_card_customer_notification,

        -- ===== AMOUNT DETAILS =====
        SAFE_CAST(JSON_VALUE(data_json, '$.amount_details.tip.amount') AS INT64)
            AS amount_details_tip_amount,

        -- ===== PAYMENT METHOD OPTIONS (CARD) =====
        JSON_VALUE(data_json, '$.payment_method_options.card.request_three_d_secure')
            AS pm_options_card_request_3ds,
        JSON_VALUE(data_json, '$.payment_method_options.card.network')
            AS pm_options_card_network,
        JSON_VALUE(data_json, '$.payment_method_options.card.installments.enabled')
            AS pm_options_card_installments_enabled,

        -- ===== ERROR =====
        JSON_VALUE(data_json, '$.last_payment_error.code') AS last_payment_error_code,
        JSON_VALUE(data_json, '$.last_payment_error.message') AS last_payment_error_message,
        JSON_VALUE(data_json, '$.last_payment_error.type') AS last_payment_error_type,
        JSON_VALUE(data_json, '$.last_payment_error.decline_code') AS last_payment_error_decline_code,
        JSON_VALUE(data_json, '$.last_payment_error.charge') AS last_payment_error_charge_id,
        JSON_VALUE(data_json, '$.last_payment_error.payment_method') AS last_payment_error_payment_method_id,
        JSON_VALUE(data_json, '$.last_payment_error.payment_method_type') AS last_payment_error_pm_type,

        TIMESTAMP_SECONDS(
            SAFE_CAST(JSON_VALUE(data_json, '$.last_payment_error.created') AS INT64)
        ) AS last_payment_error_created_at,

        -- ===== TIME =====
        TIMESTAMP_SECONDS(
            SAFE_CAST(JSON_VALUE(data_json, '$.created') AS INT64)
        ) AS created_at,

        TIMESTAMP_SECONDS(
            SAFE_CAST(JSON_VALUE(data_json, '$.canceled_at') AS INT64)
        ) AS canceled_at

    FROM source
)

SELECT * FROM parsed