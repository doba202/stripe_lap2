WITH source AS (
    SELECT
        open_id,
        data_json
    FROM {{ source('stripe_stg', 'stripe_raw_payment_intents') }}
),

parsed AS (
    SELECT
        open_id,

        -- ===== SCALAR FIELDS (level 1) =====
        JSON_VALUE(data_json, '$.id')                               AS id,
        JSON_VALUE(data_json, '$.object')                           AS object,
        SAFE_CAST(JSON_VALUE(data_json, '$.amount') AS INT64)       AS amount,
        SAFE_CAST(JSON_VALUE(data_json, '$.amount_capturable') AS INT64) AS amount_capturable,
        SAFE_CAST(JSON_VALUE(data_json, '$.amount_received') AS INT64)   AS amount_received,
        JSON_VALUE(data_json, '$.application')                      AS application,
        SAFE_CAST(JSON_VALUE(data_json, '$.application_fee_amount') AS INT64) AS application_fee_amount,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.canceled_at') AS INT64)) AS canceled_at,
        JSON_VALUE(data_json, '$.cancellation_reason')              AS cancellation_reason,
        JSON_VALUE(data_json, '$.capture_method')                   AS capture_method,
        JSON_VALUE(data_json, '$.client_secret')                    AS client_secret,
        JSON_VALUE(data_json, '$.confirmation_method')              AS confirmation_method,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.created') AS INT64)) AS created,
        JSON_VALUE(data_json, '$.currency')                         AS currency,
        JSON_VALUE(data_json, '$.customer')                         AS customer,
        JSON_VALUE(data_json, '$.customer_account')                 AS customer_account,
        JSON_VALUE(data_json, '$.description')                      AS description,
        JSON_VALUE(data_json, '$.invoice')                          AS invoice,
        JSON_VALUE(data_json, '$.latest_charge')                    AS latest_charge,
        CAST(JSON_VALUE(data_json, '$.livemode') AS BOOL)           AS livemode,
        JSON_VALUE(data_json, '$.on_behalf_of')                     AS on_behalf_of,
        JSON_VALUE(data_json, '$.payment_method')                   AS payment_method,
        JSON_VALUE(data_json, '$.receipt_email')                    AS receipt_email,
        JSON_VALUE(data_json, '$.review')                           AS review,
        JSON_VALUE(data_json, '$.setup_future_usage')               AS setup_future_usage,
        JSON_VALUE(data_json, '$.statement_descriptor')             AS statement_descriptor,
        JSON_VALUE(data_json, '$.statement_descriptor_suffix')      AS statement_descriptor_suffix,
        JSON_VALUE(data_json, '$.status')                           AS status,
        JSON_VALUE(data_json, '$.transfer_group')                   AS transfer_group,

        -- ===== amount_details (object → flatten scalars; line_items list → JSON) =====
        SAFE_CAST(JSON_VALUE(data_json, '$.amount_details.discount_amount') AS INT64)           AS amount_details_discount_amount,
        JSON_VALUE(data_json, '$.amount_details.error.code')                                    AS amount_details_error_code,
        JSON_VALUE(data_json, '$.amount_details.error.message')                                 AS amount_details_error_message,
        SAFE_CAST(JSON_VALUE(data_json, '$.amount_details.shipping.amount') AS INT64)           AS amount_details_shipping_amount,
        JSON_VALUE(data_json, '$.amount_details.shipping.from_postal_code')                     AS amount_details_shipping_from_postal_code,
        JSON_VALUE(data_json, '$.amount_details.shipping.to_postal_code')                       AS amount_details_shipping_to_postal_code,
        SAFE_CAST(JSON_VALUE(data_json, '$.amount_details.tax.total_tax_amount') AS INT64)      AS amount_details_tax_total_tax_amount,
        SAFE_CAST(JSON_VALUE(data_json, '$.amount_details.tip.amount') AS INT64)                AS amount_details_tip_amount,
        -- line_items is a paginated list object → keep JSON
        JSON_QUERY(data_json, '$.amount_details.line_items')                                    AS amount_details_line_items,

        -- ===== automatic_payment_methods (object → flatten) =====
        JSON_VALUE(data_json, '$.automatic_payment_methods.allow_redirects')    AS automatic_payment_methods_allow_redirects,
        CAST(JSON_VALUE(data_json, '$.automatic_payment_methods.enabled') AS BOOL) AS automatic_payment_methods_enabled,

        -- ===== hooks (object → flatten scalars) =====
        JSON_VALUE(data_json, '$.hooks.inputs.tax.calculation')                 AS hooks_inputs_tax_calculation,

        -- ===== last_payment_error (object → flatten scalars; payment_method + source → JSON) =====
        JSON_VALUE(data_json, '$.last_payment_error.advice_code')               AS last_payment_error_advice_code,
        JSON_VALUE(data_json, '$.last_payment_error.charge')                    AS last_payment_error_charge,
        JSON_VALUE(data_json, '$.last_payment_error.code')                      AS last_payment_error_code,
        JSON_VALUE(data_json, '$.last_payment_error.decline_code')              AS last_payment_error_decline_code,
        JSON_VALUE(data_json, '$.last_payment_error.doc_url')                   AS last_payment_error_doc_url,
        JSON_VALUE(data_json, '$.last_payment_error.message')                   AS last_payment_error_message,
        JSON_VALUE(data_json, '$.last_payment_error.network_advice_code')       AS last_payment_error_network_advice_code,
        JSON_VALUE(data_json, '$.last_payment_error.network_decline_code')      AS last_payment_error_network_decline_code,
        JSON_VALUE(data_json, '$.last_payment_error.param')                     AS last_payment_error_param,
        JSON_VALUE(data_json, '$.last_payment_error.payment_method_type')       AS last_payment_error_payment_method_type,
        JSON_VALUE(data_json, '$.last_payment_error.type')                      AS last_payment_error_type,
        -- payment_method & source are deeply nested complex objects → keep JSON
        JSON_QUERY(data_json, '$.last_payment_error.payment_method')            AS last_payment_error_payment_method,
        JSON_QUERY(data_json, '$.last_payment_error.source')                    AS last_payment_error_source,

        -- ===== payment_details (object → flatten) =====
        JSON_VALUE(data_json, '$.payment_details.customer_reference')           AS payment_details_customer_reference,
        JSON_VALUE(data_json, '$.payment_details.order_reference')              AS payment_details_order_reference,

        -- ===== payment_method_configuration_details (object → flatten) =====
        JSON_VALUE(data_json, '$.payment_method_configuration_details.id')      AS payment_method_configuration_details_id,
        JSON_VALUE(data_json, '$.payment_method_configuration_details.parent')  AS payment_method_configuration_details_parent,

        -- ===== shipping (object → flatten, incl. nested address) =====
        JSON_VALUE(data_json, '$.shipping.name')                                AS shipping_name,
        JSON_VALUE(data_json, '$.shipping.phone')                               AS shipping_phone,
        JSON_VALUE(data_json, '$.shipping.carrier')                             AS shipping_carrier,
        JSON_VALUE(data_json, '$.shipping.tracking_number')                     AS shipping_tracking_number,
        JSON_VALUE(data_json, '$.shipping.address.city')                        AS shipping_address_city,
        JSON_VALUE(data_json, '$.shipping.address.country')                     AS shipping_address_country,
        JSON_VALUE(data_json, '$.shipping.address.line1')                       AS shipping_address_line1,
        JSON_VALUE(data_json, '$.shipping.address.line2')                       AS shipping_address_line2,
        JSON_VALUE(data_json, '$.shipping.address.postal_code')                 AS shipping_address_postal_code,
        JSON_VALUE(data_json, '$.shipping.address.state')                       AS shipping_address_state,

        -- ===== transfer_data (object → flatten) =====
        SAFE_CAST(JSON_VALUE(data_json, '$.transfer_data.amount') AS INT64)     AS transfer_data_amount,
        JSON_VALUE(data_json, '$.transfer_data.destination')                    AS transfer_data_destination,

        -- ===== ARRAY / COMPLEX FIELDS — keep JSON =====
        -- excluded_payment_method_types: array of enums
        JSON_QUERY(data_json, '$.excluded_payment_method_types')        AS excluded_payment_method_types,
        -- metadata: free-form key-value
        JSON_QUERY(data_json, '$.metadata')                             AS metadata,
        -- next_action: polymorphic object with many sub-types
        JSON_QUERY(data_json, '$.next_action')                          AS next_action,
        -- payment_method_options: complex object with ~40+ payment method sub-hashes
        JSON_QUERY(data_json, '$.payment_method_options')               AS payment_method_options,
        -- payment_method_types: array of enums
        JSON_QUERY(data_json, '$.payment_method_types')                 AS payment_method_types,
        -- processing: payment processing state details
        JSON_QUERY(data_json, '$.processing')                           AS processing

    FROM source
)

SELECT * FROM parsed