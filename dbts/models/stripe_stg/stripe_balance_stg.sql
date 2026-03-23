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
)

SELECT
    open_id,
    call_at,
    available,
    pending,
    connect_reserved,
    instant_available,
    issuing,
    refund_and_dispute_prefunding

FROM source