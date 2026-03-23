WITH source AS (
    SELECT
        open_id,
        data_json
    FROM {{ source('stripe_stg', 'stripe_raw_refunds') }}
),

parsed AS (
    SELECT
        open_id,

        -- ===== SCALAR FIELDS (level 1) =====
        JSON_VALUE(data_json, '$.id')                       AS id,
        JSON_VALUE(data_json, '$.object')                   AS object,
        SAFE_CAST(JSON_VALUE(data_json, '$.amount') AS INT64) AS amount,
        JSON_VALUE(data_json, '$.balance_transaction')      AS balance_transaction,
        JSON_VALUE(data_json, '$.charge')                   AS charge,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.created') AS INT64)) AS created,
        JSON_VALUE(data_json, '$.currency')                 AS currency,
        JSON_VALUE(data_json, '$.description')              AS description,
        JSON_VALUE(data_json, '$.failure_balance_transaction') AS failure_balance_transaction,
        JSON_VALUE(data_json, '$.failure_reason')           AS failure_reason,
        JSON_VALUE(data_json, '$.instructions_email')       AS instructions_email,
        JSON_VALUE(data_json, '$.payment_intent')           AS payment_intent,
        JSON_VALUE(data_json, '$.pending_reason')           AS pending_reason,
        JSON_VALUE(data_json, '$.reason')                   AS reason,
        JSON_VALUE(data_json, '$.receipt_number')           AS receipt_number,
        JSON_VALUE(data_json, '$.source_transfer_reversal') AS source_transfer_reversal,
        JSON_VALUE(data_json, '$.status')                   AS status,
        JSON_VALUE(data_json, '$.transfer_reversal')        AS transfer_reversal,

        -- ===== destination_details (object → flatten type only; sub-objects dynamic per payment method → JSON) =====
        JSON_VALUE(data_json, '$.destination_details.type') AS destination_details_type,
        JSON_QUERY(data_json, '$.destination_details')      AS destination_details,

        -- ===== next_action (object → flatten) =====
        JSON_VALUE(data_json, '$.next_action.type')                                                 AS next_action_type,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.next_action.display_details.expires_at') AS INT64)) AS next_action_display_details_expires_at,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.next_action.display_details.email_sent.email_sent_at') AS INT64)) AS next_action_display_details_email_sent_at,
        JSON_VALUE(data_json, '$.next_action.display_details.email_sent.email_sent_to')             AS next_action_display_details_email_sent_to,

        -- ===== ARRAY / COMPLEX FIELDS — giữ nguyên JSON =====
        JSON_QUERY(data_json, '$.metadata')             AS metadata

    FROM source
)

SELECT * FROM parsed