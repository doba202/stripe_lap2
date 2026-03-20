{#{{ config(#}
{#    materialized='incremental',#}
{#    unique_key=['open_id', 'subscription_id', 'tax_rate_id']#}
{#) }}#}

WITH source AS (
    SELECT
        open_id,
        data_json
    FROM {{ source('stripe_stg', 'stripe_raw_subscriptions') }}
),

extracted AS (
    SELECT
        open_id,
        JSON_VALUE(data_json, '$.id') AS subscription_id,
        JSON_QUERY_ARRAY(data_json, '$.default_tax_rates') AS tax_rates
    FROM source
),

unnested AS (
    SELECT
        open_id,
        subscription_id,
        tax_rate
    FROM extracted,
    UNNEST(tax_rates) AS tax_rate
)

SELECT
    open_id,
    subscription_id,
    JSON_VALUE(tax_rate, '$.id') AS tax_rate_id

FROM unnested
WHERE JSON_VALUE(tax_rate, '$.id') IS NOT NULL