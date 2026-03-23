{{ config(unique_key=['id', 'open_id']) }}

WITH source AS (
    SELECT
        open_id,
        data_json
    FROM {{ source('stripe_stg', 'stripe_raw_subscriptions') }}
    WHERE 1=1
    {% if is_incremental() %}
        AND TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.created') AS INT64))
            >= TIMESTAMP_SECONDS({{ var('stripe_start_ts') }})
    {% endif %}
),

parsed AS (
    SELECT
        open_id,

        -- ===== SCALAR FIELDS (level 1) =====
        JSON_VALUE(data_json, '$.id')                       AS id,
        JSON_VALUE(data_json, '$.object')                   AS object,
        JSON_VALUE(data_json, '$.application')              AS application,
        SAFE_CAST(JSON_VALUE(data_json, '$.application_fee_percent') AS FLOAT64) AS application_fee_percent,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.billing_cycle_anchor') AS INT64)) AS billing_cycle_anchor,
        CAST(JSON_VALUE(data_json, '$.cancel_at_period_end') AS BOOL) AS cancel_at_period_end,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.cancel_at') AS INT64))     AS cancel_at,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.canceled_at') AS INT64))   AS canceled_at,
        JSON_VALUE(data_json, '$.collection_method')        AS collection_method,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.created') AS INT64))       AS created,
        JSON_VALUE(data_json, '$.currency')                 AS currency,
        JSON_VALUE(data_json, '$.customer')                 AS customer,
        JSON_VALUE(data_json, '$.customer_account')         AS customer_account,
        SAFE_CAST(JSON_VALUE(data_json, '$.days_until_due') AS INT64) AS days_until_due,
        JSON_VALUE(data_json, '$.default_payment_method')   AS default_payment_method,
        JSON_VALUE(data_json, '$.default_source')           AS default_source,
        JSON_VALUE(data_json, '$.description')              AS description,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.ended_at') AS INT64))      AS ended_at,
        JSON_VALUE(data_json, '$.latest_invoice')           AS latest_invoice,
        CAST(JSON_VALUE(data_json, '$.livemode') AS BOOL)   AS livemode,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.next_pending_invoice_item_invoice') AS INT64)) AS next_pending_invoice_item_invoice,
        JSON_VALUE(data_json, '$.on_behalf_of')             AS on_behalf_of,
        JSON_VALUE(data_json, '$.pending_setup_intent')     AS pending_setup_intent,
        JSON_VALUE(data_json, '$.schedule')                 AS schedule,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.start_date') AS INT64))    AS start_date,
        JSON_VALUE(data_json, '$.status')                   AS status,
        JSON_VALUE(data_json, '$.test_clock')               AS test_clock,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.trial_end') AS INT64))     AS trial_end,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.trial_start') AS INT64))   AS trial_start,

        -- ===== automatic_tax (object → flatten) =====
        CAST(JSON_VALUE(data_json, '$.automatic_tax.enabled') AS BOOL)                  AS automatic_tax_enabled,
        JSON_VALUE(data_json, '$.automatic_tax.disabled_reason')                        AS automatic_tax_disabled_reason,
        JSON_VALUE(data_json, '$.automatic_tax.liability.account')                      AS automatic_tax_liability_account,
        JSON_VALUE(data_json, '$.automatic_tax.liability.type')                         AS automatic_tax_liability_type,

        -- ===== billing_cycle_anchor_config (object → flatten) =====
        SAFE_CAST(JSON_VALUE(data_json, '$.billing_cycle_anchor_config.day_of_month') AS INT64) AS billing_cycle_anchor_config_day_of_month,
        SAFE_CAST(JSON_VALUE(data_json, '$.billing_cycle_anchor_config.hour') AS INT64)         AS billing_cycle_anchor_config_hour,
        SAFE_CAST(JSON_VALUE(data_json, '$.billing_cycle_anchor_config.minute') AS INT64)       AS billing_cycle_anchor_config_minute,
        SAFE_CAST(JSON_VALUE(data_json, '$.billing_cycle_anchor_config.month') AS INT64)        AS billing_cycle_anchor_config_month,
        SAFE_CAST(JSON_VALUE(data_json, '$.billing_cycle_anchor_config.second') AS INT64)       AS billing_cycle_anchor_config_second,

        -- ===== billing_mode (object → flatten) =====
        JSON_VALUE(data_json, '$.billing_mode.type')                                    AS billing_mode_type,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.billing_mode.updated_at') AS INT64)) AS billing_mode_updated_at,
        JSON_VALUE(data_json, '$.billing_mode.flexible.proration_discounts')            AS billing_mode_flexible_proration_discounts,

        -- ===== billing_thresholds (object → flatten) =====
        SAFE_CAST(JSON_VALUE(data_json, '$.billing_thresholds.amount_gte') AS INT64)    AS billing_thresholds_amount_gte,
        CAST(JSON_VALUE(data_json, '$.billing_thresholds.reset_billing_cycle_anchor') AS BOOL) AS billing_thresholds_reset_billing_cycle_anchor,

        -- ===== cancellation_details (object → flatten) =====
        JSON_VALUE(data_json, '$.cancellation_details.comment')                         AS cancellation_details_comment,
        JSON_VALUE(data_json, '$.cancellation_details.feedback')                        AS cancellation_details_feedback,
        JSON_VALUE(data_json, '$.cancellation_details.reason')                          AS cancellation_details_reason,

        -- ===== invoice_settings (object → flatten; account_tax_ids array → JSON) =====
        JSON_VALUE(data_json, '$.invoice_settings.issuer.type')                         AS invoice_settings_issuer_type,
        JSON_VALUE(data_json, '$.invoice_settings.issuer.account')                      AS invoice_settings_issuer_account,
        JSON_QUERY(data_json, '$.invoice_settings.account_tax_ids')                     AS invoice_settings_account_tax_ids,

        -- ===== pause_collection (object → flatten) =====
        JSON_VALUE(data_json, '$.pause_collection.behavior')                            AS pause_collection_behavior,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.pause_collection.resumes_at') AS INT64)) AS pause_collection_resumes_at,

        -- ===== payment_settings (object → fully flatten all scalar leaves) =====
        JSON_VALUE(data_json, '$.payment_settings.save_default_payment_method')         AS payment_settings_save_default_payment_method,
        -- acss_debit
        JSON_VALUE(data_json, '$.payment_settings.payment_method_options.acss_debit.mandate_options.transaction_type') AS payment_settings_pmo_acss_debit_mandate_options_transaction_type,
        JSON_VALUE(data_json, '$.payment_settings.payment_method_options.acss_debit.verification_method')              AS payment_settings_pmo_acss_debit_verification_method,
        -- bancontact
        JSON_VALUE(data_json, '$.payment_settings.payment_method_options.bancontact.preferred_language')               AS payment_settings_pmo_bancontact_preferred_language,
        -- card
        SAFE_CAST(JSON_VALUE(data_json, '$.payment_settings.payment_method_options.card.mandate_options.amount') AS INT64) AS payment_settings_pmo_card_mandate_options_amount,
        JSON_VALUE(data_json, '$.payment_settings.payment_method_options.card.mandate_options.amount_type')            AS payment_settings_pmo_card_mandate_options_amount_type,
        JSON_VALUE(data_json, '$.payment_settings.payment_method_options.card.mandate_options.description')            AS payment_settings_pmo_card_mandate_options_description,
        JSON_VALUE(data_json, '$.payment_settings.payment_method_options.card.network')                                AS payment_settings_pmo_card_network,
        JSON_VALUE(data_json, '$.payment_settings.payment_method_options.card.request_three_d_secure')                 AS payment_settings_pmo_card_request_three_d_secure,
        -- customer_balance
        JSON_VALUE(data_json, '$.payment_settings.payment_method_options.customer_balance.funding_type')               AS payment_settings_pmo_customer_balance_funding_type,
        JSON_VALUE(data_json, '$.payment_settings.payment_method_options.customer_balance.bank_transfer.type')         AS payment_settings_pmo_customer_balance_bank_transfer_type,
        JSON_VALUE(data_json, '$.payment_settings.payment_method_options.customer_balance.bank_transfer.eu_bank_transfer.country') AS payment_settings_pmo_customer_balance_bank_transfer_eu_country,
        -- payto
        SAFE_CAST(JSON_VALUE(data_json, '$.payment_settings.payment_method_options.payto.mandate_options.amount') AS INT64) AS payment_settings_pmo_payto_mandate_options_amount,
        JSON_VALUE(data_json, '$.payment_settings.payment_method_options.payto.mandate_options.amount_type')           AS payment_settings_pmo_payto_mandate_options_amount_type,
        JSON_VALUE(data_json, '$.payment_settings.payment_method_options.payto.mandate_options.purpose')               AS payment_settings_pmo_payto_mandate_options_purpose,
        -- upi
        SAFE_CAST(JSON_VALUE(data_json, '$.payment_settings.payment_method_options.upi.mandate_options.amount') AS INT64) AS payment_settings_pmo_upi_mandate_options_amount,
        JSON_VALUE(data_json, '$.payment_settings.payment_method_options.upi.mandate_options.amount_type')             AS payment_settings_pmo_upi_mandate_options_amount_type,
        JSON_VALUE(data_json, '$.payment_settings.payment_method_options.upi.mandate_options.description')             AS payment_settings_pmo_upi_mandate_options_description,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.payment_settings.payment_method_options.upi.mandate_options.end_date') AS INT64)) AS payment_settings_pmo_upi_mandate_options_end_date,
        -- us_bank_account
        JSON_VALUE(data_json, '$.payment_settings.payment_method_options.us_bank_account.verification_method')         AS payment_settings_pmo_us_bank_account_verification_method,
        -- us_bank_account financial_connections: arrays → keep JSON
        JSON_QUERY(data_json, '$.payment_settings.payment_method_options.us_bank_account.financial_connections.filters.account_subcategories') AS payment_settings_pmo_us_bank_account_fc_account_subcategories,
        JSON_QUERY(data_json, '$.payment_settings.payment_method_options.us_bank_account.financial_connections.permissions') AS payment_settings_pmo_us_bank_account_fc_permissions,
        JSON_QUERY(data_json, '$.payment_settings.payment_method_options.us_bank_account.financial_connections.prefetch')    AS payment_settings_pmo_us_bank_account_fc_prefetch,
        -- payment_method_types: array of enums → keep JSON
        JSON_QUERY(data_json, '$.payment_settings.payment_method_types')                AS payment_settings_payment_method_types,

        -- ===== pending_invoice_item_interval (object → flatten) =====
        JSON_VALUE(data_json, '$.pending_invoice_item_interval.interval')               AS pending_invoice_item_interval_interval,
        SAFE_CAST(JSON_VALUE(data_json, '$.pending_invoice_item_interval.interval_count') AS INT64) AS pending_invoice_item_interval_interval_count,

        -- ===== pending_update (object → flatten scalars; subscription_items array → JSON) =====
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.pending_update.billing_cycle_anchor') AS INT64)) AS pending_update_billing_cycle_anchor,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.pending_update.expires_at') AS INT64))           AS pending_update_expires_at,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.pending_update.trial_end') AS INT64))            AS pending_update_trial_end,
        CAST(JSON_VALUE(data_json, '$.pending_update.trial_from_plan') AS BOOL)                               AS pending_update_trial_from_plan,
        JSON_QUERY(data_json, '$.pending_update.subscription_items')                    AS pending_update_subscription_items,

        -- ===== presentment_details (object → flatten) =====
        JSON_VALUE(data_json, '$.presentment_details.presentment_currency')             AS presentment_details_presentment_currency,

        -- ===== transfer_data (object → flatten) =====
        SAFE_CAST(JSON_VALUE(data_json, '$.transfer_data.amount_percent') AS FLOAT64)   AS transfer_data_amount_percent,
        JSON_VALUE(data_json, '$.transfer_data.destination')                            AS transfer_data_destination,

        -- ===== trial_settings (object → flatten) =====
        JSON_VALUE(data_json, '$.trial_settings.end_behavior.missing_payment_method')   AS trial_settings_end_behavior_missing_payment_method,

        -- ===== ARRAY / COMPLEX FIELDS — giữ nguyên JSON =====
        -- default_tax_rates: array of TaxRate objects
        JSON_QUERY(data_json, '$.default_tax_rates')        AS default_tax_rates,
        -- discounts: array of strings
        JSON_QUERY(data_json, '$.discounts')                AS discounts,
        -- items: Stripe List object (items.data is an array of subscription_item objects)
        JSON_QUERY(data_json, '$.items')                    AS items,
        -- metadata: free-form key-value
        JSON_QUERY(data_json, '$.metadata')                 AS metadata

    FROM source
)

SELECT * FROM parsed