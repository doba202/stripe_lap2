WITH source AS (
    SELECT open_id,data_json
    FROM {{ source('stripe_stg', 'stripe_raw_invoices') }}
),

parsed AS (
    SELECT
        open_id,
        -- ===================== PRIMARY =====================
        JSON_VALUE(data_json, '$.id') AS invoice_id,
        JSON_VALUE(data_json, '$.number') AS invoice_number,
        JSON_VALUE(data_json, '$.receipt_number') AS receipt_number,
        JSON_VALUE(data_json, '$.description') AS description,

        -- ===================== CUSTOMER =====================
        JSON_VALUE(data_json, '$.customer') AS customer_id,
        JSON_VALUE(data_json, '$.customer_tax_exempt') AS customer_tax_exempt,

        -- ===================== APPLICATION =====================
        JSON_VALUE(data_json, '$.application') AS application_id,

        -- ===================== STATUS =====================
        JSON_VALUE(data_json, '$.status') AS status,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.status_transitions.finalized_at') AS INT64)) AS finalized_at,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.status_transitions.marked_uncollectible_at') AS INT64)) AS marked_uncollectible_at,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.status_transitions.paid_at') AS INT64)) AS paid_at,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.status_transitions.voided_at') AS INT64)) AS voided_at,

        -- ===================== AMOUNTS =====================
        SAFE_CAST(JSON_VALUE(data_json, '$.starting_balance') AS INT64) AS starting_balance,
        SAFE_CAST(JSON_VALUE(data_json, '$.ending_balance') AS INT64) AS ending_balance,
        SAFE_CAST(JSON_VALUE(data_json, '$.amount_due') AS INT64) AS amount_due,
        SAFE_CAST(JSON_VALUE(data_json, '$.amount_paid') AS INT64) AS amount_paid,
        SAFE_CAST(JSON_VALUE(data_json, '$.amount_remaining') AS INT64) AS amount_remaining,
        SAFE_CAST(JSON_VALUE(data_json, '$.amount_overpaid') AS INT64) AS amount_overpaid,
        SAFE_CAST(JSON_VALUE(data_json, '$.subtotal') AS INT64) AS subtotal,
        SAFE_CAST(JSON_VALUE(data_json, '$.subtotal_excluding_tax') AS INT64) AS subtotal_excluding_tax,
        SAFE_CAST(JSON_VALUE(data_json, '$.total_excluding_tax') AS INT64) AS total_excluding_tax,
        SAFE_CAST(JSON_VALUE(data_json, '$.total') AS INT64) AS total,
        SAFE_CAST(JSON_VALUE(data_json, '$.amount_shipping') AS INT64) AS amount_shipping,
        SAFE_CAST(JSON_VALUE(data_json, '$.post_payment_credit_notes_amount') AS INT64) AS post_payment_credit_notes_amount,
        SAFE_CAST(JSON_VALUE(data_json, '$.pre_payment_credit_notes_amount') AS INT64) AS pre_payment_credit_notes_amount,
        JSON_VALUE(data_json, '$.currency') AS currency,

        -- ===================== BILLING =====================
        JSON_VALUE(data_json, '$.billing_reason') AS billing_reason,
        JSON_VALUE(data_json, '$.collection_method') AS collection_method,
        SAFE_CAST(JSON_VALUE(data_json, '$.attempt_count') AS INT64) AS attempt_count,
        CAST(JSON_VALUE(data_json, '$.attempted') AS BOOL) AS attempted,
        CAST(JSON_VALUE(data_json, '$.auto_advance') AS BOOL) AS auto_advance,

        -- ===================== TIME =====================
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.created') AS INT64)) AS created_at,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.period_start') AS INT64)) AS period_start,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.period_end') AS INT64)) AS period_end,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.due_date') AS INT64)) AS due_date,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.next_payment_attempt') AS INT64)) AS next_payment_attempt,

        -- ===================== TAX =====================
        CAST(JSON_VALUE(data_json, '$.automatic_tax.enabled') AS BOOL) AS automatic_tax_enabled,
        JSON_VALUE(data_json, '$.automatic_tax.status') AS automatic_tax_status,
        JSON_VALUE(data_json, '$.automatic_tax.disabled_reason') AS automatic_tax_disabled_reason,
        JSON_VALUE(data_json, '$.automatic_tax.provider') AS automatic_tax_provider,
        JSON_VALUE(data_json, '$.automatic_tax.liability.account') AS tax_liability_account_id,
        JSON_VALUE(data_json, '$.automatic_tax.liability.type') AS tax_liability_type,

        -- ===================== PAYMENT =====================
        JSON_VALUE(data_json, '$.default_payment_method') AS default_payment_method,
        JSON_VALUE(data_json, '$.default_source') AS default_source,
        JSON_VALUE(data_json, '$.confirmation_secret.client_secret') AS confirmation_client_secret,
        JSON_VALUE(data_json, '$.confirmation_secret.type') AS confirmation_type,

        -- ===================== HOSTED LINKS =====================
        JSON_VALUE(data_json, '$.hosted_invoice_url') AS hosted_invoice_url,
        JSON_VALUE(data_json, '$.invoice_pdf') AS invoice_pdf,

        -- ===================== SHIPPING =====================
        SAFE_CAST(JSON_VALUE(data_json, '$.shipping_cost.amount_subtotal') AS INT64) AS shipping_amount_subtotal,
        SAFE_CAST(JSON_VALUE(data_json, '$.shipping_cost.amount_tax') AS INT64) AS shipping_amount_tax,
        SAFE_CAST(JSON_VALUE(data_json, '$.shipping_cost.amount_total') AS INT64) AS shipping_amount_total,
        JSON_VALUE(data_json, '$.shipping_cost.shipping_rate') AS shipping_rate_id,

        -- ===================== TRANSFER =====================
        JSON_VALUE(data_json, '$.transfer_data.destination') AS transfer_destination_account_id,
        SAFE_CAST(JSON_VALUE(data_json, '$.transfer_data.amount') AS INT64) AS transfer_amount,

        -- ===================== ISSUER =====================
        JSON_VALUE(data_json, '$.issuer.account') AS issuer_account_id,
        JSON_VALUE(data_json, '$.issuer.type') AS issuer_type,

        -- ===================== ERROR =====================
        JSON_VALUE(data_json, '$.last_finalization_error.code') AS finalization_error_code,
        JSON_VALUE(data_json, '$.last_finalization_error.message') AS finalization_error_message,
        JSON_VALUE(data_json, '$.last_finalization_error.type') AS finalization_error_type,

        -- ===================== PARENT =====================
        JSON_VALUE(data_json, '$.parent.type') AS parent_type,
        JSON_VALUE(data_json, '$.parent.quote_details.quote') AS parent_quote_id,
        JSON_VALUE(data_json, '$.parent.subscription_details.subscription') AS parent_subscription_id,
        TIMESTAMP_SECONDS(
            SAFE_CAST(JSON_VALUE(data_json, '$.parent.subscription_details.subscription_proration_date') AS INT64)
        ) AS parent_subscription_proration_date,
        JSON_QUERY(data_json, '$.parent.subscription_details.metadata') AS parent_subscription_metadata_json,

        -- ===================== OTHER =====================
        JSON_VALUE(data_json, '$.statement_descriptor') AS statement_descriptor,
        JSON_VALUE(data_json, '$.footer') AS footer,
        JSON_VALUE(data_json, '$.account_country') AS account_country,
        JSON_VALUE(data_json, '$.account_name') AS account_name,
        JSON_VALUE(data_json, '$.on_behalf_of') AS on_behalf_of,
        JSON_VALUE(data_json, '$.test_clock') AS test_clock_id,
        JSON_VALUE(data_json, '$.latest_revision') AS latest_revision,

        -- ===================== META =====================
        CAST(JSON_VALUE(data_json, '$.livemode') AS BOOL) AS livemode

    FROM source
)

SELECT *
FROM parsed