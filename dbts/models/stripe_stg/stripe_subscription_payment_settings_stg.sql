WITH source AS (
    SELECT
        JSON_VALUE(data_json, '$.id') AS subscription_id,
        data_json
    FROM {{ source('stripe_stg', 'stripe_raw_subscriptions') }}
)

SELECT
    subscription_id,

    -- core
    JSON_VALUE(data_json, '$.payment_settings.save_default_payment_method')
        AS save_default_payment_method,

    -- flatten nhẹ
    JSON_VALUE(data_json, '$.payment_settings.payment_method_options.card.network')
        AS card_network,

    JSON_VALUE(data_json, '$.payment_settings.payment_method_options.card.request_three_d_secure')
        AS request_three_d_secure,

    JSON_VALUE(data_json, '$.payment_settings.payment_method_options.acss_debit.verification_method')
        AS acss_verification_method,

    JSON_VALUE(data_json, '$.payment_settings.payment_method_options.bancontact.preferred_language')
        AS bancontact_language,

    -- raw fallback (VERY IMPORTANT)
    TO_JSON_STRING(JSON_QUERY(data_json, '$.payment_settings.payment_method_options'))
        AS payment_method_options_json

FROM source