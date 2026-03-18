WITH source AS (
    SELECT
        id,
        call_at,
        data_json
    FROM {{ source('stripe_stg', 'stripe_raw_customers') }}
),

parsed AS (
    SELECT
        id,
        call_at,

        -- basic fields
        JSON_VALUE(data_json, '$.id') AS customer_id,
        JSON_VALUE(data_json, '$.email') AS email,
        JSON_VALUE(data_json, '$.currency') AS currency,
        JSON_VALUE(data_json, '$.description') AS description,
        JSON_VALUE(data_json, '$.phone') AS phone,

        -- booleans
        CAST(JSON_VALUE(data_json, '$.delinquent') AS BOOL) AS delinquent,
        CAST(JSON_VALUE(data_json, '$.livemode') AS BOOL) AS livemode,

        -- timestamps (epoch → timestamp)
        TIMESTAMP_SECONDS(CAST(JSON_VALUE(data_json, '$.created') AS INT64)) AS created_at,

        -- address
        JSON_VALUE(data_json, '$.address.city') AS city,
        JSON_VALUE(data_json, '$.address.country') AS country,
        JSON_VALUE(data_json, '$.address.postal_code') AS postal_code,

        -- invoice settings
        JSON_VALUE(data_json, '$.invoice_settings.default_payment_method') AS default_payment_method,

        -- metadata (nested object)
        JSON_VALUE(data_json, '$.metadata.ff_funnel') AS ff_funnel,
        JSON_VALUE(data_json, '$.metadata.ff_session_id') AS ff_session_id

    FROM source
)

SELECT * FROM parsed