WITH source AS (
    SELECT
        open_id,
        data_json
    FROM {{ source('stripe_stg', 'stripe_raw_charges') }}
),

parsed AS (
    SELECT
        open_id,
        -- ===== PRIMARY =====
        JSON_VALUE(data_json, '$.id') AS charge_id,
        JSON_VALUE(data_json, '$.payment_method_details.type') AS payment_method_type,

        -- ===== CARD INFO =====
        JSON_VALUE(data_json, '$.payment_method_details.card.brand') AS card_brand,
        JSON_VALUE(data_json, '$.payment_method_details.card.funding') AS card_funding,
        JSON_VALUE(data_json, '$.payment_method_details.card.country') AS card_country,
        JSON_VALUE(data_json, '$.payment_method_details.card.network') AS card_network,

        -- ===== CARD CHECKS =====
        JSON_VALUE(data_json, '$.payment_method_details.card.checks.cvc_check') AS card_cvc_check,
        JSON_VALUE(data_json, '$.payment_method_details.card.checks.address_postal_code_check')
            AS card_postal_check
    FROM source
)

SELECT * FROM parsed