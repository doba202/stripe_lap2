{{ config(unique_key=['open_id', 'call_date']) }}

WITH source AS (
    SELECT
        open_id,
        call_at,
        available,
        pending,
        connect_reserved,
        instant_available,
        issuing,
        refund_and_dispute_prefunding
    FROM {{ source('stripe_stg', 'stripe_raw_balance') }}
    WHERE 1=1
    {% if is_incremental() %}
        AND call_at >= DATETIME(TIMESTAMP_SECONDS({{ var('stripe_start_ts') }}))
    {% endif %}
)

SELECT
    open_id,
    call_at,
    DATE(call_at) AS call_date,
    available,
    pending,
    connect_reserved,
    instant_available,
    issuing,
    refund_and_dispute_prefunding

FROM source