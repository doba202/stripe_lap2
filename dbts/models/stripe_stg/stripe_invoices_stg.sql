WITH source AS (
    SELECT open_id, data_json
    FROM {{ source('stripe_stg', 'stripe_raw_invoices') }}
),

parsed AS (
    SELECT
        open_id,

        -- ===== SCALAR FIELDS (level 1) =====
        JSON_VALUE(data_json, '$.id')                   AS id,
        JSON_VALUE(data_json, '$.object')               AS object,
        JSON_VALUE(data_json, '$.account_country')      AS account_country,
        JSON_VALUE(data_json, '$.account_name')         AS account_name,
        SAFE_CAST(JSON_VALUE(data_json, '$.amount_due') AS INT64)                       AS amount_due,
        SAFE_CAST(JSON_VALUE(data_json, '$.amount_overpaid') AS INT64)                  AS amount_overpaid,
        SAFE_CAST(JSON_VALUE(data_json, '$.amount_paid') AS INT64)                      AS amount_paid,
        SAFE_CAST(JSON_VALUE(data_json, '$.amount_remaining') AS INT64)                 AS amount_remaining,
        SAFE_CAST(JSON_VALUE(data_json, '$.amount_shipping') AS INT64)                  AS amount_shipping,
        JSON_VALUE(data_json, '$.application')          AS application,
        SAFE_CAST(JSON_VALUE(data_json, '$.attempt_count') AS INT64)                    AS attempt_count,
        CAST(JSON_VALUE(data_json, '$.attempted') AS BOOL)                              AS attempted,
        CAST(JSON_VALUE(data_json, '$.auto_advance') AS BOOL)                           AS auto_advance,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.automatically_finalizes_at') AS INT64)) AS automatically_finalizes_at,
        JSON_VALUE(data_json, '$.billing_reason')       AS billing_reason,
        JSON_VALUE(data_json, '$.collection_method')    AS collection_method,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.created') AS INT64))       AS created,
        JSON_VALUE(data_json, '$.currency')             AS currency,
        JSON_VALUE(data_json, '$.customer')             AS customer,
        JSON_VALUE(data_json, '$.customer_account')     AS customer_account,
        JSON_VALUE(data_json, '$.customer_email')       AS customer_email,
        JSON_VALUE(data_json, '$.customer_name')        AS customer_name,
        JSON_VALUE(data_json, '$.customer_phone')       AS customer_phone,
        JSON_VALUE(data_json, '$.customer_tax_exempt')  AS customer_tax_exempt,
        JSON_VALUE(data_json, '$.default_payment_method') AS default_payment_method,
        JSON_VALUE(data_json, '$.default_source')       AS default_source,
        JSON_VALUE(data_json, '$.description')          AS description,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.due_date') AS INT64))      AS due_date,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.effective_at') AS INT64))  AS effective_at,
        SAFE_CAST(JSON_VALUE(data_json, '$.ending_balance') AS INT64)                   AS ending_balance,
        JSON_VALUE(data_json, '$.footer')               AS footer,
        JSON_VALUE(data_json, '$.hosted_invoice_url')   AS hosted_invoice_url,
        JSON_VALUE(data_json, '$.invoice_pdf')          AS invoice_pdf,
        JSON_VALUE(data_json, '$.latest_revision')      AS latest_revision,
        CAST(JSON_VALUE(data_json, '$.livemode') AS BOOL)                               AS livemode,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.next_payment_attempt') AS INT64)) AS next_payment_attempt,
        JSON_VALUE(data_json, '$.number')               AS number,
        JSON_VALUE(data_json, '$.on_behalf_of')         AS on_behalf_of,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.period_end') AS INT64))    AS period_end,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.period_start') AS INT64))  AS period_start,
        SAFE_CAST(JSON_VALUE(data_json, '$.post_payment_credit_notes_amount') AS INT64) AS post_payment_credit_notes_amount,
        SAFE_CAST(JSON_VALUE(data_json, '$.pre_payment_credit_notes_amount') AS INT64)  AS pre_payment_credit_notes_amount,
        JSON_VALUE(data_json, '$.receipt_number')       AS receipt_number,
        SAFE_CAST(JSON_VALUE(data_json, '$.starting_balance') AS INT64)                 AS starting_balance,
        JSON_VALUE(data_json, '$.statement_descriptor') AS statement_descriptor,
        JSON_VALUE(data_json, '$.status')               AS status,
        SAFE_CAST(JSON_VALUE(data_json, '$.subtotal') AS INT64)                         AS subtotal,
        SAFE_CAST(JSON_VALUE(data_json, '$.subtotal_excluding_tax') AS INT64)           AS subtotal_excluding_tax,
        JSON_VALUE(data_json, '$.test_clock')           AS test_clock,
        SAFE_CAST(JSON_VALUE(data_json, '$.total') AS INT64)                            AS total,
        SAFE_CAST(JSON_VALUE(data_json, '$.total_excluding_tax') AS INT64)              AS total_excluding_tax,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.webhooks_delivered_at') AS INT64)) AS webhooks_delivered_at,

        -- ===== automatic_tax (object → flatten) =====
        CAST(JSON_VALUE(data_json, '$.automatic_tax.enabled') AS BOOL)  AS automatic_tax_enabled,
        JSON_VALUE(data_json, '$.automatic_tax.status')                 AS automatic_tax_status,
        JSON_VALUE(data_json, '$.automatic_tax.disabled_reason')        AS automatic_tax_disabled_reason,
        JSON_VALUE(data_json, '$.automatic_tax.liability.type')         AS automatic_tax_liability_type,
        JSON_VALUE(data_json, '$.automatic_tax.liability.account')      AS automatic_tax_liability_account,
        JSON_VALUE(data_json, '$.automatic_tax.provider')               AS automatic_tax_provider,

        -- ===== confirmation_secret (object → flatten) =====
        JSON_VALUE(data_json, '$.confirmation_secret.client_secret')    AS confirmation_secret_client_secret,
        JSON_VALUE(data_json, '$.confirmation_secret.type')             AS confirmation_secret_type,

        -- ===== customer_address (object → flatten) =====
        JSON_VALUE(data_json, '$.customer_address.city')        AS customer_address_city,
        JSON_VALUE(data_json, '$.customer_address.country')     AS customer_address_country,
        JSON_VALUE(data_json, '$.customer_address.line1')       AS customer_address_line1,
        JSON_VALUE(data_json, '$.customer_address.line2')       AS customer_address_line2,
        JSON_VALUE(data_json, '$.customer_address.postal_code') AS customer_address_postal_code,
        JSON_VALUE(data_json, '$.customer_address.state')       AS customer_address_state,

        -- ===== customer_shipping (object → flatten, incl. nested address) =====
        JSON_VALUE(data_json, '$.customer_shipping.name')                   AS customer_shipping_name,
        JSON_VALUE(data_json, '$.customer_shipping.phone')                  AS customer_shipping_phone,
        JSON_VALUE(data_json, '$.customer_shipping.address.city')           AS customer_shipping_address_city,
        JSON_VALUE(data_json, '$.customer_shipping.address.country')        AS customer_shipping_address_country,
        JSON_VALUE(data_json, '$.customer_shipping.address.line1')          AS customer_shipping_address_line1,
        JSON_VALUE(data_json, '$.customer_shipping.address.line2')          AS customer_shipping_address_line2,
        JSON_VALUE(data_json, '$.customer_shipping.address.postal_code')    AS customer_shipping_address_postal_code,
        JSON_VALUE(data_json, '$.customer_shipping.address.state')          AS customer_shipping_address_state,

        -- ===== from_invoice (object → flatten) =====
        JSON_VALUE(data_json, '$.from_invoice.action')  AS from_invoice_action,
        JSON_VALUE(data_json, '$.from_invoice.invoice') AS from_invoice_invoice,

        -- ===== issuer (object → flatten) =====
        JSON_VALUE(data_json, '$.issuer.type')          AS issuer_type,
        JSON_VALUE(data_json, '$.issuer.account')       AS issuer_account,

        -- ===== last_finalization_error (object → flatten) =====
        JSON_VALUE(data_json, '$.last_finalization_error.advice_code')          AS last_finalization_error_advice_code,
        JSON_VALUE(data_json, '$.last_finalization_error.code')                 AS last_finalization_error_code,
        JSON_VALUE(data_json, '$.last_finalization_error.doc_url')              AS last_finalization_error_doc_url,
        JSON_VALUE(data_json, '$.last_finalization_error.message')              AS last_finalization_error_message,
        JSON_VALUE(data_json, '$.last_finalization_error.network_advice_code')  AS last_finalization_error_network_advice_code,
        JSON_VALUE(data_json, '$.last_finalization_error.network_decline_code') AS last_finalization_error_network_decline_code,
        JSON_VALUE(data_json, '$.last_finalization_error.param')                AS last_finalization_error_param,
        JSON_VALUE(data_json, '$.last_finalization_error.payment_method_type')  AS last_finalization_error_payment_method_type,
        JSON_VALUE(data_json, '$.last_finalization_error.type')                 AS last_finalization_error_type,

        -- ===== parent (object → flatten) =====
        JSON_VALUE(data_json, '$.parent.type')                                              AS parent_type,
        JSON_VALUE(data_json, '$.parent.quote_details.quote')                               AS parent_quote_details_quote,
        JSON_VALUE(data_json, '$.parent.subscription_details.subscription')                 AS parent_subscription_details_subscription,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.parent.subscription_details.subscription_proration_date') AS INT64)) AS parent_subscription_details_subscription_proration_date,
        -- subscription_details.metadata is a free-form map → keep JSON
        JSON_QUERY(data_json, '$.parent.subscription_details.metadata')                     AS parent_subscription_details_metadata,

        -- ===== payment_settings (object → flatten) =====
        JSON_VALUE(data_json, '$.payment_settings.default_mandate')                         AS payment_settings_default_mandate,
        -- payment_method_options sub-fields
        JSON_VALUE(data_json, '$.payment_settings.payment_method_options.acss_debit.mandate_options.transaction_type') AS payment_settings_acss_debit_mandate_options_transaction_type,
        JSON_VALUE(data_json, '$.payment_settings.payment_method_options.acss_debit.verification_method')              AS payment_settings_acss_debit_verification_method,
        JSON_VALUE(data_json, '$.payment_settings.payment_method_options.bancontact.preferred_language')                AS payment_settings_bancontact_preferred_language,
        CAST(JSON_VALUE(data_json, '$.payment_settings.payment_method_options.card.installments.enabled') AS BOOL)     AS payment_settings_card_installments_enabled,
        JSON_VALUE(data_json, '$.payment_settings.payment_method_options.card.request_three_d_secure')                 AS payment_settings_card_request_three_d_secure,
        JSON_VALUE(data_json, '$.payment_settings.payment_method_options.customer_balance.bank_transfer.eu_bank_transfer.country') AS payment_settings_customer_balance_bank_transfer_eu_bank_transfer_country,
        JSON_VALUE(data_json, '$.payment_settings.payment_method_options.customer_balance.bank_transfer.type')          AS payment_settings_customer_balance_bank_transfer_type,
        JSON_VALUE(data_json, '$.payment_settings.payment_method_options.customer_balance.funding_type')                AS payment_settings_customer_balance_funding_type,
        SAFE_CAST(JSON_VALUE(data_json, '$.payment_settings.payment_method_options.payto.mandate_options.amount') AS INT64) AS payment_settings_payto_mandate_options_amount,
        JSON_VALUE(data_json, '$.payment_settings.payment_method_options.payto.mandate_options.amount_type')            AS payment_settings_payto_mandate_options_amount_type,
        JSON_VALUE(data_json, '$.payment_settings.payment_method_options.payto.mandate_options.purpose')                AS payment_settings_payto_mandate_options_purpose,
        SAFE_CAST(JSON_VALUE(data_json, '$.payment_settings.payment_method_options.upi.mandate_options.amount') AS INT64) AS payment_settings_upi_mandate_options_amount,
        JSON_VALUE(data_json, '$.payment_settings.payment_method_options.upi.mandate_options.amount_type')              AS payment_settings_upi_mandate_options_amount_type,
        JSON_VALUE(data_json, '$.payment_settings.payment_method_options.upi.mandate_options.description')              AS payment_settings_upi_mandate_options_description,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.payment_settings.payment_method_options.upi.mandate_options.end_date') AS INT64)) AS payment_settings_upi_mandate_options_end_date,
        JSON_VALUE(data_json, '$.payment_settings.payment_method_options.us_bank_account.verification_method')          AS payment_settings_us_bank_account_verification_method,
        -- financial_connections arrays → keep JSON
        JSON_QUERY(data_json, '$.payment_settings.payment_method_options.us_bank_account.financial_connections.filters.account_subcategories') AS payment_settings_us_bank_account_financial_connections_filters_account_subcategories,
        JSON_QUERY(data_json, '$.payment_settings.payment_method_options.us_bank_account.financial_connections.permissions') AS payment_settings_us_bank_account_financial_connections_permissions,
        JSON_QUERY(data_json, '$.payment_settings.payment_method_options.us_bank_account.financial_connections.prefetch')    AS payment_settings_us_bank_account_financial_connections_prefetch,
        -- payment_method_types is an array of enums → keep JSON
        JSON_QUERY(data_json, '$.payment_settings.payment_method_types')                    AS payment_settings_payment_method_types,

        -- ===== rendering (object → flatten) =====
        JSON_VALUE(data_json, '$.rendering.amount_tax_display')     AS rendering_amount_tax_display,
        JSON_VALUE(data_json, '$.rendering.pdf.page_size')          AS rendering_pdf_page_size,
        JSON_VALUE(data_json, '$.rendering.template')               AS rendering_template,
        SAFE_CAST(JSON_VALUE(data_json, '$.rendering.template_version') AS INT64) AS rendering_template_version,

        -- ===== shipping_cost (object → flatten scalars, keep taxes array as JSON) =====
        SAFE_CAST(JSON_VALUE(data_json, '$.shipping_cost.amount_subtotal') AS INT64)    AS shipping_cost_amount_subtotal,
        SAFE_CAST(JSON_VALUE(data_json, '$.shipping_cost.amount_tax') AS INT64)         AS shipping_cost_amount_tax,
        SAFE_CAST(JSON_VALUE(data_json, '$.shipping_cost.amount_total') AS INT64)       AS shipping_cost_amount_total,
        JSON_VALUE(data_json, '$.shipping_cost.shipping_rate')                          AS shipping_cost_shipping_rate,
        JSON_QUERY(data_json, '$.shipping_cost.taxes')                                  AS shipping_cost_taxes,

        -- ===== shipping_details (object → flatten, incl. nested address) =====
        JSON_VALUE(data_json, '$.shipping_details.name')                    AS shipping_details_name,
        JSON_VALUE(data_json, '$.shipping_details.phone')                   AS shipping_details_phone,
        JSON_VALUE(data_json, '$.shipping_details.address.city')            AS shipping_details_address_city,
        JSON_VALUE(data_json, '$.shipping_details.address.country')         AS shipping_details_address_country,
        JSON_VALUE(data_json, '$.shipping_details.address.line1')           AS shipping_details_address_line1,
        JSON_VALUE(data_json, '$.shipping_details.address.line2')           AS shipping_details_address_line2,
        JSON_VALUE(data_json, '$.shipping_details.address.postal_code')     AS shipping_details_address_postal_code,
        JSON_VALUE(data_json, '$.shipping_details.address.state')           AS shipping_details_address_state,

        -- ===== status_transitions (object → flatten) =====
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.status_transitions.finalized_at') AS INT64))           AS status_transitions_finalized_at,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.status_transitions.marked_uncollectible_at') AS INT64)) AS status_transitions_marked_uncollectible_at,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.status_transitions.paid_at') AS INT64))                AS status_transitions_paid_at,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.status_transitions.voided_at') AS INT64))              AS status_transitions_voided_at,

        -- ===== threshold_reason (object → flatten scalars, item_reasons array → JSON) =====
        SAFE_CAST(JSON_VALUE(data_json, '$.threshold_reason.amount_gte') AS INT64)  AS threshold_reason_amount_gte,
        JSON_QUERY(data_json, '$.threshold_reason.item_reasons')                    AS threshold_reason_item_reasons,

        -- ===== transfer_data (object → flatten) =====
        SAFE_CAST(JSON_VALUE(data_json, '$.transfer_data.amount') AS INT64) AS transfer_data_amount,
        JSON_VALUE(data_json, '$.transfer_data.destination')                AS transfer_data_destination,

        -- ===== ARRAY / COMPLEX FIELDS — keep JSON =====
        JSON_QUERY(data_json, '$.account_tax_ids')              AS account_tax_ids,
        JSON_QUERY(data_json, '$.custom_fields')                AS custom_fields,
        JSON_QUERY(data_json, '$.customer_tax_ids')             AS customer_tax_ids,
        JSON_QUERY(data_json, '$.default_tax_rates')            AS default_tax_rates,
        JSON_QUERY(data_json, '$.discounts')                    AS discounts,
        JSON_QUERY(data_json, '$.lines')                        AS lines,
        JSON_QUERY(data_json, '$.metadata')                     AS metadata,
        JSON_QUERY(data_json, '$.payments')                     AS payments,
        JSON_QUERY(data_json, '$.total_discount_amounts')       AS total_discount_amounts,
        JSON_QUERY(data_json, '$.total_pretax_credit_amounts')  AS total_pretax_credit_amounts,
        JSON_QUERY(data_json, '$.total_taxes')                  AS total_taxes

    FROM source
)

SELECT * FROM parsed