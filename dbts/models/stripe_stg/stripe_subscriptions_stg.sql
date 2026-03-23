WITH source AS (
    SELECT
        open_id,
        data_json
    FROM {{ source('stripe_stg', 'stripe_raw_subscriptions') }}
),

parsed AS (
    SELECT
        open_id,
        -- ===== PRIMARY =====
        JSON_VALUE(data_json, '$.id') AS subscription_id,

        -- ===== CUSTOMER =====
        JSON_VALUE(data_json, '$.customer') AS customer_id,
        JSON_VALUE(data_json, '$.customer_account') AS customer_account_id,

        -- ===== STATUS =====
        JSON_VALUE(data_json, '$.status') AS status,
        CAST(JSON_VALUE(data_json, '$.livemode') AS BOOL) AS is_livemode,

        -- ===== COLLECTION =====
        JSON_VALUE(data_json, '$.collection_method') AS collection_method,
        SAFE_CAST(JSON_VALUE(data_json, '$.days_until_due') AS INT64) AS days_until_due,
        JSON_VALUE(data_json, '$.currency') AS currency,

        -- ===== DESCRIPTION =====
        JSON_VALUE(data_json, '$.description') AS description,

        -- ===== APP =====
        JSON_VALUE(data_json, '$.application') AS application_id,
        SAFE_CAST(JSON_VALUE(data_json, '$.application_fee_percent') AS FLOAT64) AS application_fee_percent,

        -- ===== PAYMENT =====
        JSON_VALUE(data_json, '$.default_payment_method') AS default_payment_method_id,
        JSON_VALUE(data_json, '$.default_source') AS default_source_id,

        -- ===== INVOICE =====
        JSON_VALUE(data_json, '$.latest_invoice') AS latest_invoice_id,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.next_pending_invoice_item_invoice') AS INT64))
            AS next_pending_invoice_at,

        -- ===== INVOICE SETTINGS =====
        JSON_VALUE(data_json, '$.invoice_settings.issuer.type') AS invoice_issuer_type,
        JSON_VALUE(data_json, '$.invoice_settings.issuer.account') AS invoice_issuer_account_id,
        ARRAY(
            SELECT JSON_VALUE(x)
            FROM UNNEST(JSON_QUERY_ARRAY(data_json, '$.invoice_settings.account_tax_ids')) AS x
        ) AS invoice_account_tax_ids,

        -- ===== BILLING CYCLE =====
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.billing_cycle_anchor') AS INT64))
            AS billing_cycle_anchor_at,
        SAFE_CAST(JSON_VALUE(data_json, '$.billing_cycle_anchor_config.day_of_month') AS INT64)
            AS billing_anchor_day_of_month,
        SAFE_CAST(JSON_VALUE(data_json, '$.billing_cycle_anchor_config.hour') AS INT64)
            AS billing_anchor_hour,
        SAFE_CAST(JSON_VALUE(data_json, '$.billing_cycle_anchor_config.minute') AS INT64)
            AS billing_anchor_minute,
        SAFE_CAST(JSON_VALUE(data_json, '$.billing_cycle_anchor_config.month') AS INT64)
            AS billing_anchor_month,
        SAFE_CAST(JSON_VALUE(data_json, '$.billing_cycle_anchor_config.second') AS INT64)
            AS billing_anchor_second,

        -- ===== BILLING MODE =====
        JSON_VALUE(data_json, '$.billing_mode.type') AS billing_mode_type,
        JSON_VALUE(data_json, '$.billing_mode.flexible.proration_discounts') AS billing_proration_discounts,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.billing_mode.updated_at') AS INT64))
            AS billing_mode_updated_at,

        -- ===== BILLING THRESHOLDS =====
        SAFE_CAST(JSON_VALUE(data_json, '$.billing_thresholds.amount_gte') AS INT64)
            AS billing_threshold_amount_gte,
        CAST(JSON_VALUE(data_json, '$.billing_thresholds.reset_billing_cycle_anchor') AS BOOL)
            AS billing_threshold_reset_anchor,

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
        JSON_VALUE(data_json, '$.cancellation_details.feedback') AS cancellation_feedback,
        JSON_VALUE(data_json, '$.cancellation_details.comment') AS cancellation_comment,

        -- ===== AUTOMATIC TAX =====
        CAST(JSON_VALUE(data_json, '$.automatic_tax.enabled') AS BOOL) AS automatic_tax_enabled,
        JSON_VALUE(data_json, '$.automatic_tax.disabled_reason') AS automatic_tax_disabled_reason,
        JSON_VALUE(data_json, '$.automatic_tax.liability.type') AS automatic_tax_liability_type,
        JSON_VALUE(data_json, '$.automatic_tax.liability.account') AS automatic_tax_liability_account_id,

        -- ===== DEFAULT TAX RATES (chỉ lưu id — chi tiết join từ bảng tax_rates) =====
        ARRAY(
            SELECT AS STRUCT
                JSON_VALUE(tr, '$.id') AS tax_rate_id
            FROM UNNEST(JSON_QUERY_ARRAY(data_json, '$.default_tax_rates')) AS tr
        ) AS default_tax_rate_ids,

        -- ===== DISCOUNTS (discount ids) =====
        ARRAY(
            SELECT JSON_VALUE(d)
            FROM UNNEST(JSON_QUERY_ARRAY(data_json, '$.discounts')) AS d
        ) AS discount_ids,

        -- items: skip — dùng bảng stripe_raw_subscription_items riêng, join qua subscription_id

        -- ===== PAUSE COLLECTION =====
        JSON_VALUE(data_json, '$.pause_collection.behavior') AS pause_collection_behavior,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.pause_collection.resumes_at') AS INT64))
            AS pause_collection_resumes_at,

        -- ===== PENDING INVOICE ITEM INTERVAL =====
        JSON_VALUE(data_json, '$.pending_invoice_item_interval.interval') AS pending_invoice_interval,
        SAFE_CAST(JSON_VALUE(data_json, '$.pending_invoice_item_interval.interval_count') AS INT64)
            AS pending_invoice_interval_count,

        -- ===== PAYMENT SETTINGS =====
        JSON_VALUE(data_json, '$.payment_settings.save_default_payment_method')
            AS save_default_payment_method,
        ARRAY(
            SELECT JSON_VALUE(x)
            FROM UNNEST(JSON_QUERY_ARRAY(data_json, '$.payment_settings.payment_method_types')) AS x
        ) AS payment_method_types,
        JSON_QUERY(data_json, '$.payment_settings.payment_method_options') AS payment_method_options_json,

        -- ===== CONNECT =====
        JSON_VALUE(data_json, '$.on_behalf_of') AS on_behalf_of,
        SAFE_CAST(JSON_VALUE(data_json, '$.transfer_data.amount_percent') AS FLOAT64) AS transfer_amount_percent,
        JSON_VALUE(data_json, '$.transfer_data.destination') AS transfer_destination,

        -- ===== TRIAL =====
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.trial_start') AS INT64))
            AS trial_start,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.trial_end') AS INT64))
            AS trial_end,
        JSON_VALUE(data_json, '$.trial_settings.end_behavior.missing_payment_method')
            AS trial_missing_payment_method_behavior,

        -- ===== PRESENTMENT =====
        JSON_VALUE(data_json, '$.presentment_details.presentment_currency') AS presentment_currency,

        -- ===== SCHEDULE / PENDING =====
        JSON_VALUE(data_json, '$.schedule') AS schedule_id,
        JSON_VALUE(data_json, '$.pending_setup_intent') AS pending_setup_intent_id,
        JSON_QUERY(data_json, '$.pending_update') AS pending_update_json,

        -- ===== TEST =====
        JSON_VALUE(data_json, '$.test_clock') AS test_clock_id,

        -- ===== METADATA =====
        JSON_QUERY(data_json, '$.metadata') AS metadata_json,

        -- ===== DATES =====
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.start_date') AS INT64))
            AS start_date,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.created') AS INT64))
            AS created_at

    FROM source
)

SELECT * FROM parsed