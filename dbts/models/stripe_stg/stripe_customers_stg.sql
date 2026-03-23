{{ config(unique_key=['id', 'open_id']) }}

WITH source AS (
    SELECT
        open_id,
        data_json
    FROM {{ source('stripe_stg', 'stripe_raw_customers') }}
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
        JSON_VALUE(data_json, '$.id')                   AS id,
        JSON_VALUE(data_json, '$.object')               AS object,
        JSON_VALUE(data_json, '$.name')                 AS name,
        JSON_VALUE(data_json, '$.email')                AS email,
        JSON_VALUE(data_json, '$.phone')                AS phone,
        JSON_VALUE(data_json, '$.description')          AS description,
        JSON_VALUE(data_json, '$.currency')             AS currency,
        SAFE_CAST(JSON_VALUE(data_json, '$.balance') AS INT64)  AS balance,
        JSON_VALUE(data_json, '$.business_name')        AS business_name,
        JSON_VALUE(data_json, '$.individual_name')      AS individual_name,
        CAST(JSON_VALUE(data_json, '$.delinquent') AS BOOL)     AS delinquent,
        JSON_VALUE(data_json, '$.tax_exempt')           AS tax_exempt,
        CAST(JSON_VALUE(data_json, '$.livemode') AS BOOL)       AS livemode,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.created') AS INT64)) AS created,
        JSON_VALUE(data_json, '$.invoice_prefix')       AS invoice_prefix,
        SAFE_CAST(JSON_VALUE(data_json, '$.next_invoice_sequence') AS INT64) AS next_invoice_sequence,
        JSON_VALUE(data_json, '$.default_source')       AS default_source,
        JSON_VALUE(data_json, '$.customer_account')     AS customer_account,
        JSON_VALUE(data_json, '$.test_clock')           AS test_clock,

        -- ===== address (object → flatten) =====
        JSON_VALUE(data_json, '$.address.city')         AS address_city,
        JSON_VALUE(data_json, '$.address.country')      AS address_country,
        JSON_VALUE(data_json, '$.address.line1')        AS address_line1,
        JSON_VALUE(data_json, '$.address.line2')        AS address_line2,
        JSON_VALUE(data_json, '$.address.postal_code')  AS address_postal_code,
        JSON_VALUE(data_json, '$.address.state')        AS address_state,

        -- ===== cash_balance (object → flatten scalars) =====
        JSON_VALUE(data_json, '$.cash_balance.object')                          AS cash_balance_object,
        JSON_VALUE(data_json, '$.cash_balance.customer')                        AS cash_balance_customer,
        JSON_VALUE(data_json, '$.cash_balance.customer_account')                AS cash_balance_customer_account,
        CAST(JSON_VALUE(data_json, '$.cash_balance.livemode') AS BOOL)          AS cash_balance_livemode,
        JSON_VALUE(data_json, '$.cash_balance.settings.reconciliation_mode')    AS cash_balance_settings_reconciliation_mode,
        CAST(JSON_VALUE(data_json, '$.cash_balance.settings.using_merchant_default') AS BOOL) AS cash_balance_settings_using_merchant_default,
        -- cash_balance.available is dynamic (currency keys) → keep JSON
        JSON_QUERY(data_json, '$.cash_balance.available')                       AS cash_balance_available,

        -- ===== discount (object → flatten) =====
        JSON_VALUE(data_json, '$.discount.id')                  AS discount_id,
        JSON_VALUE(data_json, '$.discount.object')              AS discount_object,
        JSON_VALUE(data_json, '$.discount.checkout_session')    AS discount_checkout_session,
        JSON_VALUE(data_json, '$.discount.customer')            AS discount_customer,
        JSON_VALUE(data_json, '$.discount.customer_account')    AS discount_customer_account,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.discount.end') AS INT64))   AS discount_end,
        JSON_VALUE(data_json, '$.discount.invoice')             AS discount_invoice,
        JSON_VALUE(data_json, '$.discount.invoice_item')        AS discount_invoice_item,
        JSON_VALUE(data_json, '$.discount.promotion_code')      AS discount_promotion_code,
        JSON_VALUE(data_json, '$.discount.source.coupon')       AS discount_source_coupon,
        JSON_VALUE(data_json, '$.discount.source.type')         AS discount_source_type,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.discount.start') AS INT64)) AS discount_start,
        JSON_VALUE(data_json, '$.discount.subscription')        AS discount_subscription,
        JSON_VALUE(data_json, '$.discount.subscription_item')   AS discount_subscription_item,

        -- ===== invoice_settings (object → flatten) =====
        JSON_VALUE(data_json, '$.invoice_settings.default_payment_method')                  AS invoice_settings_default_payment_method,
        JSON_VALUE(data_json, '$.invoice_settings.footer')                                  AS invoice_settings_footer,
        JSON_VALUE(data_json, '$.invoice_settings.rendering_options.amount_tax_display')    AS invoice_settings_rendering_options_amount_tax_display,
        JSON_VALUE(data_json, '$.invoice_settings.rendering_options.template')              AS invoice_settings_rendering_options_template,
        -- custom_fields is an array → keep JSON
        JSON_QUERY(data_json, '$.invoice_settings.custom_fields')                           AS invoice_settings_custom_fields,

        -- ===== shipping (object → flatten, including nested shipping.address) =====
        JSON_VALUE(data_json, '$.shipping.name')                AS shipping_name,
        JSON_VALUE(data_json, '$.shipping.phone')               AS shipping_phone,
        JSON_VALUE(data_json, '$.shipping.address.city')        AS shipping_address_city,
        JSON_VALUE(data_json, '$.shipping.address.country')     AS shipping_address_country,
        JSON_VALUE(data_json, '$.shipping.address.line1')       AS shipping_address_line1,
        JSON_VALUE(data_json, '$.shipping.address.line2')       AS shipping_address_line2,
        JSON_VALUE(data_json, '$.shipping.address.postal_code') AS shipping_address_postal_code,
        JSON_VALUE(data_json, '$.shipping.address.state')       AS shipping_address_state,

        -- ===== tax (object → flatten) =====
        JSON_VALUE(data_json, '$.tax.automatic_tax')        AS tax_automatic_tax,
        JSON_VALUE(data_json, '$.tax.ip_address')           AS tax_ip_address,
        JSON_VALUE(data_json, '$.tax.location.country')     AS tax_location_country,
        JSON_VALUE(data_json, '$.tax.location.source')      AS tax_location_source,
        JSON_VALUE(data_json, '$.tax.location.state')       AS tax_location_state,
        JSON_VALUE(data_json, '$.tax.provider')             AS tax_provider,

        -- ===== DYNAMIC / ARRAY FIELDS — keep JSON =====
        -- invoice_credit_balance: dynamic object (keys = currency codes)
        JSON_QUERY(data_json, '$.invoice_credit_balance')   AS invoice_credit_balance,
        -- metadata: specific fields extracted
        JSON_VALUE(data_json, '$.metadata.ff_funnel')       AS metadata_ff_funnel,
        JSON_VALUE(data_json, '$.metadata.ff_session_id')   AS metadata_ff_session_id,
        JSON_QUERY(data_json, '$.metadata')                 AS metadata,
        -- preferred_locales: array of strings
        JSON_QUERY(data_json, '$.preferred_locales')        AS preferred_locales,
        -- sources: complex nested array
        JSON_QUERY(data_json, '$.sources')                  AS sources,
        -- subscriptions: complex nested array
        JSON_QUERY(data_json, '$.subscriptions')            AS subscriptions,
        -- tax_ids: complex nested array
        JSON_QUERY(data_json, '$.tax_ids')                  AS tax_ids

    FROM source
)

SELECT * FROM parsed