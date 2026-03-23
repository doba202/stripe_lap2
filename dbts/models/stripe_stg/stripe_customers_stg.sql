WITH source AS (
    SELECT
        open_id,
        data_json
    FROM {{ source('stripe_stg', 'stripe_raw_customers') }}
),

parsed AS (
    SELECT
        open_id,
        -- ===== PRIMARY =====
        JSON_VALUE(data_json, '$.id') AS customer_id,

        -- ===== BASIC INFO =====
        JSON_VALUE(data_json, '$.name') AS name,
        JSON_VALUE(data_json, '$.email') AS email,
        JSON_VALUE(data_json, '$.phone') AS phone,
        JSON_VALUE(data_json, '$.description') AS description,
        JSON_VALUE(data_json, '$.business_name') AS business_name,
        JSON_VALUE(data_json, '$.individual_name') AS individual_name,
        JSON_VALUE(data_json, '$.currency') AS currency,
        SAFE_CAST(JSON_VALUE(data_json, '$.balance') AS INT64) AS balance,

        -- ===== STATUS =====
        CAST(JSON_VALUE(data_json, '$.delinquent') AS BOOL) AS is_delinquent,
        JSON_VALUE(data_json, '$.tax_exempt') AS tax_exempt,

        -- ===== ADDRESS =====
        JSON_VALUE(data_json, '$.address.city') AS address_city,
        JSON_VALUE(data_json, '$.address.country') AS address_country,
        JSON_VALUE(data_json, '$.address.line1') AS address_line1,
        JSON_VALUE(data_json, '$.address.line2') AS address_line2,
        JSON_VALUE(data_json, '$.address.postal_code') AS address_postal_code,
        JSON_VALUE(data_json, '$.address.state') AS address_state,

        -- ===== SHIPPING =====
        JSON_VALUE(data_json, '$.shipping.name') AS shipping_name,
        JSON_VALUE(data_json, '$.shipping.phone') AS shipping_phone,
        JSON_VALUE(data_json, '$.shipping.address.city') AS shipping_city,
        JSON_VALUE(data_json, '$.shipping.address.country') AS shipping_country,
        JSON_VALUE(data_json, '$.shipping.address.line1') AS shipping_line1,
        JSON_VALUE(data_json, '$.shipping.address.line2') AS shipping_line2,
        JSON_VALUE(data_json, '$.shipping.address.postal_code') AS shipping_postal_code,
        JSON_VALUE(data_json, '$.shipping.address.state') AS shipping_state,

        -- ===== PAYMENT =====
        JSON_VALUE(data_json, '$.default_source') AS default_source_id,
        JSON_VALUE(data_json, '$.invoice_settings.default_payment_method') AS default_payment_method_id,

        -- ===== INVOICE SETTINGS =====
        JSON_VALUE(data_json, '$.invoice_prefix') AS invoice_prefix,
        SAFE_CAST(JSON_VALUE(data_json, '$.next_invoice_sequence') AS INT64) AS next_invoice_sequence,
        JSON_VALUE(data_json, '$.invoice_settings.footer') AS invoice_footer,
        JSON_VALUE(data_json, '$.invoice_settings.rendering_options.amount_tax_display') AS invoice_amount_tax_display,
        JSON_VALUE(data_json, '$.invoice_settings.rendering_options.template') AS invoice_rendering_template,

        -- invoice_settings.custom_fields (RECORD)
        ARRAY(
            SELECT AS STRUCT
                JSON_VALUE(cf, '$.name') AS name,
                JSON_VALUE(cf, '$.value') AS value
            FROM UNNEST(JSON_QUERY_ARRAY(data_json, '$.invoice_settings.custom_fields')) AS cf
        ) AS invoice_custom_fields,

        -- ===== DISCOUNT (flatten — chi tiết coupon join từ bảng coupons) =====
        JSON_VALUE(data_json, '$.discount.id') AS discount_id,
        JSON_VALUE(data_json, '$.discount.source.coupon') AS discount_coupon_id,
        JSON_VALUE(data_json, '$.discount.source.type') AS discount_source_type,
        JSON_VALUE(data_json, '$.discount.promotion_code') AS discount_promotion_code,
        JSON_VALUE(data_json, '$.discount.subscription') AS discount_subscription_id,
        JSON_VALUE(data_json, '$.discount.checkout_session') AS discount_checkout_session,
        JSON_VALUE(data_json, '$.discount.invoice') AS discount_invoice_id,
        JSON_VALUE(data_json, '$.discount.invoice_item') AS discount_invoice_item_id,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.discount.start') AS INT64)) AS discount_start,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.discount.end') AS INT64)) AS discount_end,

        -- ===== TAX =====
        JSON_VALUE(data_json, '$.tax.automatic_tax') AS tax_automatic_status,
        JSON_VALUE(data_json, '$.tax.ip_address') AS tax_ip_address,
        JSON_VALUE(data_json, '$.tax.location.country') AS tax_location_country,
        JSON_VALUE(data_json, '$.tax.location.state') AS tax_location_state,
        JSON_VALUE(data_json, '$.tax.location.source') AS tax_location_source,
        JSON_VALUE(data_json, '$.tax.provider') AS tax_provider,

        -- tax_ids (RECORD — chỉ lưu id, type, value, country; chi tiết join từ bảng tax_ids)
        ARRAY(
            SELECT AS STRUCT
                JSON_VALUE(ti, '$.id') AS tax_id,
                JSON_VALUE(ti, '$.type') AS type,
                JSON_VALUE(ti, '$.value') AS value,
                JSON_VALUE(ti, '$.country') AS country,
                JSON_VALUE(ti, '$.verification.status') AS verification_status
            FROM UNNEST(JSON_QUERY_ARRAY(data_json, '$.tax_ids.data')) AS ti
        ) AS tax_ids,

        -- ===== CASH BALANCE =====
        JSON_VALUE(data_json, '$.cash_balance.settings.reconciliation_mode') AS cash_balance_reconciliation_mode,

        -- ===== ACCOUNT =====
        JSON_VALUE(data_json, '$.customer_account') AS customer_account_id,
        JSON_VALUE(data_json, '$.test_clock') AS test_clock_id,

        -- ===== PREFERRED LOCALES =====
        ARRAY(
            SELECT JSON_VALUE(x)
            FROM UNNEST(JSON_QUERY_ARRAY(data_json, '$.preferred_locales')) AS x
        ) AS preferred_locales,

        -- ===== METADATA (full JSON) =====
        JSON_QUERY(data_json, '$.metadata') AS metadata_json,

        -- ===== CREATED =====
        TIMESTAMP_SECONDS(
            SAFE_CAST(JSON_VALUE(data_json, '$.created') AS INT64)
        ) AS created_at

    FROM source
)

SELECT * FROM parsed