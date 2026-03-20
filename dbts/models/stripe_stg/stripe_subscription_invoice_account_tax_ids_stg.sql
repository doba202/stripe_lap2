{{ config(
    materialized='incremental',
    unique_key=['open_id', 'subscription_id', 'account_tax_id']
) }}

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
        JSON_QUERY_ARRAY(data_json, '$.invoice_settings.account_tax_ids') AS account_tax_ids
    FROM source
),

unnested AS (
    SELECT
        open_id,
        subscription_id,
        account_tax_id
    FROM extracted,
    UNNEST(account_tax_ids) AS account_tax_id
)

SELECT
    open_id,
    subscription_id,
    -- vì đây là array string → cần JSON_VALUE
    JSON_VALUE(account_tax_id) AS account_tax_id

FROM unnested
WHERE JSON_VALUE(account_tax_id) IS NOT NULL