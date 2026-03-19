WITH source AS (
    SELECT
        data_json
    FROM {{ source('stripe_stg', 'stripe_raw_subscriptions') }}
),

parsed AS (
    SELECT
        -- ===== PRIMARY =====
        JSON_VALUE(data_json, '$.id') AS subscription_id,

        -- ===== CUSTOMER =====
        JSON_VALUE(data_json, '$.customer') AS customer_id,

        -- ===== STATUS =====
        JSON_VALUE(data_json, '$.status') AS status,

        -- ===== COLLECTION =====
        JSON_VALUE(data_json, '$.collection_method') AS collection_method,
        SAFE_CAST(JSON_VALUE(data_json, '$.days_until_due') AS INT64) AS days_until_due,

        -- ===== CURRENCY =====
        JSON_VALUE(data_json, '$.currency') AS currency,

        -- ===== PAYMENT =====
        JSON_VALUE(data_json, '$.default_payment_method') AS default_payment_method_id,
        JSON_VALUE(data_json, '$.default_source') AS default_source_id,

        -- ===== INVOICE =====
        JSON_VALUE(data_json, '$.latest_invoice') AS latest_invoice_id,
        JSON_VALUE(data_json, '$.next_pending_invoice_item_invoice') AS next_pending_invoice_item_invoice,

        -- ===== BILLING =====
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.billing_cycle_anchor') AS INT64))
            AS billing_cycle_anchor_at,

        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.start_date') AS INT64))
            AS start_date,

        -- ===== CANCEL =====
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.cancel_at') AS INT64))
            AS cancel_at,

        CAST(JSON_VALUE(data_json, '$.cancel_at_period_end') AS BOOL)
            AS cancel_at_period_end,

        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.canceled_at') AS INT64))
            AS canceled_at,

        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.ended_at') AS INT64))
            AS ended_at,

        -- ===== CANCELLATION DETAILS =====
        JSON_VALUE(data_json, '$.cancellation_details.reason') AS cancellation_reason,
        JSON_VALUE(data_json, '$.cancellation_details.comment') AS cancellation_comment,
        JSON_VALUE(data_json, '$.cancellation_details.feedback') AS cancellation_feedback,

        -- ===== AUTOMATIC TAX =====
        CAST(JSON_VALUE(data_json, '$.automatic_tax.enabled') AS BOOL)
            AS automatic_tax_enabled,

        JSON_VALUE(data_json, '$.automatic_tax.liability') AS automatic_tax_liability,

        -- ===== INVOICE SETTINGS =====
        JSON_VALUE(data_json, '$.invoice_settings.issuer.type') AS invoice_issuer_type,

        -- ===== PAYMENT SETTINGS =====
        JSON_VALUE(data_json, '$.payment_settings.save_default_payment_method')
            AS save_default_payment_method,

        -- ===== TRIAL =====
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.trial_start') AS INT64))
            AS trial_start,

        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.trial_end') AS INT64))
            AS trial_end,

        JSON_VALUE(data_json, '$.trial_settings.end_behavior.missing_payment_method')
            AS trial_missing_payment_method_behavior,

        -- ===== CONNECT =====
        JSON_VALUE(data_json, '$.on_behalf_of') AS on_behalf_of,

        -- ===== SCHEDULE / PENDING =====
        JSON_VALUE(data_json, '$.schedule') AS schedule_id,
        JSON_VALUE(data_json, '$.pending_setup_intent') AS pending_setup_intent_id,
        JSON_VALUE(data_json, '$.pending_update') AS pending_update,

        -- ===== CREATED =====
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.created') AS INT64))
            AS created_at

    FROM source
)

SELECT * FROM parsed