WITH source AS (
    SELECT
        open_id,
        data_json
    FROM {{ source('stripe_stg', 'stripe_raw_payment_intents') }}
),

parsed AS (
    SELECT
        open_id,
        -- ===== PRIMARY =====
        JSON_VALUE(data_json, '$.id') AS payment_intent_id,

        -- ===== CUSTOMER =====
        JSON_VALUE(data_json, '$.customer') AS customer_id,
        JSON_VALUE(data_json, '$.customer_account') AS customer_account_id,

        -- ===== AMOUNT =====
        SAFE_CAST(JSON_VALUE(data_json, '$.amount') AS INT64) AS amount,
        SAFE_CAST(JSON_VALUE(data_json, '$.amount_received') AS INT64) AS amount_received,
        SAFE_CAST(JSON_VALUE(data_json, '$.amount_capturable') AS INT64) AS amount_capturable,
        JSON_VALUE(data_json, '$.currency') AS currency,

        -- ===== AMOUNT DETAILS =====
        SAFE_CAST(JSON_VALUE(data_json, '$.amount_details.discount_amount') AS INT64)
            AS amount_discount,
        SAFE_CAST(JSON_VALUE(data_json, '$.amount_details.shipping.amount') AS INT64)
            AS amount_shipping,
        JSON_VALUE(data_json, '$.amount_details.shipping.from_postal_code')
            AS shipping_from_postal_code,
        JSON_VALUE(data_json, '$.amount_details.shipping.to_postal_code')
            AS shipping_to_postal_code,
        SAFE_CAST(JSON_VALUE(data_json, '$.amount_details.tax.total_tax_amount') AS INT64)
            AS amount_tax,
        SAFE_CAST(JSON_VALUE(data_json, '$.amount_details.tip.amount') AS INT64)
            AS amount_tip,

        -- ===== STATUS =====
        JSON_VALUE(data_json, '$.status') AS status,

        -- ===== APP =====
        JSON_VALUE(data_json, '$.application') AS application_id,
        SAFE_CAST(JSON_VALUE(data_json, '$.application_fee_amount') AS INT64) AS application_fee_amount,

        -- ===== PAYMENT METHOD =====
        JSON_VALUE(data_json, '$.payment_method') AS payment_method_id,

        -- ===== LATEST CHARGE =====
        JSON_VALUE(data_json, '$.latest_charge') AS latest_charge_id,

        -- ===== INVOICE =====
        JSON_VALUE(data_json, '$.invoice') AS invoice_id,

        -- ===== CONFIG =====
        JSON_VALUE(data_json, '$.capture_method') AS capture_method,
        JSON_VALUE(data_json, '$.confirmation_method') AS confirmation_method,
        JSON_VALUE(data_json, '$.setup_future_usage') AS setup_future_usage,

        -- ===== AUTO PAYMENT =====
        CAST(JSON_VALUE(data_json, '$.automatic_payment_methods.enabled') AS BOOL)
            AS auto_payment_methods_enabled,
        JSON_VALUE(data_json, '$.automatic_payment_methods.allow_redirects')
            AS auto_payment_methods_allow_redirects,

        -- ===== DESCRIPTION / STATEMENT =====
        JSON_VALUE(data_json, '$.description') AS description,
        JSON_VALUE(data_json, '$.statement_descriptor') AS statement_descriptor,
        JSON_VALUE(data_json, '$.statement_descriptor_suffix') AS statement_descriptor_suffix,

        -- ===== PAYMENT DETAILS (L2/L3) =====
        JSON_VALUE(data_json, '$.payment_details.customer_reference') AS payment_customer_reference,
        JSON_VALUE(data_json, '$.payment_details.order_reference') AS payment_order_reference,

        -- ===== PAYMENT METHOD CONFIG =====
        JSON_VALUE(data_json, '$.payment_method_configuration_details.id')
            AS pm_config_id,
        JSON_VALUE(data_json, '$.payment_method_configuration_details.parent')
            AS pm_config_parent_id,

        -- ===== CANCELLATION =====
        JSON_VALUE(data_json, '$.cancellation_reason') AS cancellation_reason,
        TIMESTAMP_SECONDS(
            SAFE_CAST(JSON_VALUE(data_json, '$.canceled_at') AS INT64)
        ) AS canceled_at,

        -- ===== SHIPPING =====
        JSON_VALUE(data_json, '$.shipping.name') AS shipping_name,
        JSON_VALUE(data_json, '$.shipping.phone') AS shipping_phone,
        JSON_VALUE(data_json, '$.shipping.carrier') AS shipping_carrier,
        JSON_VALUE(data_json, '$.shipping.tracking_number') AS shipping_tracking_number,
        JSON_VALUE(data_json, '$.shipping.address.city') AS shipping_city,
        JSON_VALUE(data_json, '$.shipping.address.country') AS shipping_country,
        JSON_VALUE(data_json, '$.shipping.address.line1') AS shipping_line1,
        JSON_VALUE(data_json, '$.shipping.address.line2') AS shipping_line2,
        JSON_VALUE(data_json, '$.shipping.address.postal_code') AS shipping_postal_code,
        JSON_VALUE(data_json, '$.shipping.address.state') AS shipping_state,

        -- ===== CONNECT =====
        JSON_VALUE(data_json, '$.on_behalf_of') AS on_behalf_of,
        JSON_VALUE(data_json, '$.transfer_group') AS transfer_group,
        SAFE_CAST(JSON_VALUE(data_json, '$.transfer_data.amount') AS INT64) AS transfer_amount,
        JSON_VALUE(data_json, '$.transfer_data.destination') AS transfer_destination,

        -- ===== REVIEW =====
        JSON_VALUE(data_json, '$.review') AS review_id,

        -- ===== NEXT ACTION (type + full JSON vì quá phức tạp) =====
        JSON_VALUE(data_json, '$.next_action.type') AS next_action_type,
        JSON_QUERY(data_json, '$.next_action') AS next_action_json,

        -- ===== PROCESSING =====
        JSON_VALUE(data_json, '$.processing.type') AS processing_type,

        -- ===== LAST PAYMENT ERROR =====
        JSON_VALUE(data_json, '$.last_payment_error.type') AS error_type,
        JSON_VALUE(data_json, '$.last_payment_error.code') AS error_code,
        JSON_VALUE(data_json, '$.last_payment_error.message') AS error_message,
        JSON_VALUE(data_json, '$.last_payment_error.decline_code') AS error_decline_code,
        JSON_VALUE(data_json, '$.last_payment_error.advice_code') AS error_advice_code,
        JSON_VALUE(data_json, '$.last_payment_error.network_advice_code') AS error_network_advice_code,
        JSON_VALUE(data_json, '$.last_payment_error.network_decline_code') AS error_network_decline_code,
        JSON_VALUE(data_json, '$.last_payment_error.param') AS error_param,
        JSON_VALUE(data_json, '$.last_payment_error.charge') AS error_charge_id,
        JSON_VALUE(data_json, '$.last_payment_error.payment_method.id') AS error_payment_method_id,
        JSON_VALUE(data_json, '$.last_payment_error.payment_method_type') AS error_payment_method_type,

        -- ===== HOOKS =====
        JSON_VALUE(data_json, '$.hooks.inputs.tax.calculation') AS hooks_tax_calculation_id,

        -- ===== PAYMENT METHOD OPTIONS (JSON — quá phức tạp per-method) =====
        JSON_QUERY(data_json, '$.payment_method_options') AS payment_method_options_json,

        -- ===== EXCLUDED PAYMENT METHOD TYPES =====
        ARRAY(
            SELECT JSON_VALUE(x)
            FROM UNNEST(JSON_QUERY_ARRAY(data_json, '$.excluded_payment_method_types')) AS x
        ) AS excluded_payment_method_types,

        -- ===== METADATA =====
        JSON_QUERY(data_json, '$.metadata') AS metadata_json,

        -- ===== CREATED =====
        TIMESTAMP_SECONDS(
            SAFE_CAST(JSON_VALUE(data_json, '$.created') AS INT64)
        ) AS created_at

    FROM source
)

SELECT * FROM parsed