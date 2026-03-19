WITH source AS (
    SELECT
        id AS raw_id,
        data_json
    FROM {{ source('stripe_stg', 'stripe_raw_refunds') }}
),

parsed AS (
    SELECT
        -- ===== PRIMARY =====
        JSON_VALUE(data_json, '$.id') AS refund_id,
        JSON_VALUE(data_json, '$.object') AS object,

        -- ===== LINKS (CRITICAL) =====
        JSON_VALUE(data_json, '$.charge') AS charge_id,
        JSON_VALUE(data_json, '$.payment_intent') AS payment_intent_id,
        JSON_VALUE(data_json, '$.balance_transaction') AS balance_txn_id,
        JSON_VALUE(data_json, '$.failure_balance_transaction') AS failure_balance_txn_id,

        -- ===== AMOUNT =====
        SAFE_CAST(JSON_VALUE(data_json, '$.amount') AS INT64) AS amount,
        JSON_VALUE(data_json, '$.currency') AS currency,

        -- ===== STATUS =====
        JSON_VALUE(data_json, '$.status') AS status,
        JSON_VALUE(data_json, '$.pending_reason') AS pending_reason,

        -- ===== REASON =====
        JSON_VALUE(data_json, '$.reason') AS reason,
        JSON_VALUE(data_json, '$.failure_reason') AS failure_reason,

        -- ===== DESCRIPTION =====
        JSON_VALUE(data_json, '$.description') AS description,

        -- ===== RECEIPT =====
        JSON_VALUE(data_json, '$.receipt_number') AS receipt_number,

        -- ===== EMAIL / INSTRUCTION =====
        JSON_VALUE(data_json, '$.instructions_email') AS instructions_email,

        -- ===== CONNECT =====
        JSON_VALUE(data_json, '$.source_transfer_reversal') AS source_transfer_reversal_id,
        JSON_VALUE(data_json, '$.transfer_reversal') AS transfer_reversal_id,

        -- ===== DESTINATION DETAILS (CORE TYPE) =====
        JSON_VALUE(data_json, '$.destination_details.type') AS destination_type,

        -- ===== DESTINATION DETAILS - CARD =====
        JSON_VALUE(data_json, '$.destination_details.card.reference') AS dest_card_reference,
        JSON_VALUE(data_json, '$.destination_details.card.reference_status') AS dest_card_reference_status,
        JSON_VALUE(data_json, '$.destination_details.card.reference_type') AS dest_card_reference_type,
        JSON_VALUE(data_json, '$.destination_details.card.type') AS dest_card_type,

        -- ===== DESTINATION DETAILS - BLIK =====
        JSON_VALUE(data_json, '$.destination_details.blik.reference') AS dest_blik_reference,
        JSON_VALUE(data_json, '$.destination_details.blik.reference_status') AS dest_blik_reference_status,
        JSON_VALUE(data_json, '$.destination_details.blik.network_decline_code') AS dest_blik_decline_code,

        -- ===== DESTINATION DETAILS - BANK TRANSFER (GENERIC PATTERN) =====
        JSON_VALUE(data_json, '$.destination_details.br_bank_transfer.reference') AS dest_br_bank_ref,
        JSON_VALUE(data_json, '$.destination_details.br_bank_transfer.reference_status') AS dest_br_bank_ref_status,

        JSON_VALUE(data_json, '$.destination_details.eu_bank_transfer.reference') AS dest_eu_bank_ref,
        JSON_VALUE(data_json, '$.destination_details.eu_bank_transfer.reference_status') AS dest_eu_bank_ref_status,

        JSON_VALUE(data_json, '$.destination_details.gb_bank_transfer.reference') AS dest_gb_bank_ref,
        JSON_VALUE(data_json, '$.destination_details.gb_bank_transfer.reference_status') AS dest_gb_bank_ref_status,

        JSON_VALUE(data_json, '$.destination_details.jp_bank_transfer.reference') AS dest_jp_bank_ref,
        JSON_VALUE(data_json, '$.destination_details.jp_bank_transfer.reference_status') AS dest_jp_bank_ref_status,

        JSON_VALUE(data_json, '$.destination_details.mx_bank_transfer.reference') AS dest_mx_bank_ref,
        JSON_VALUE(data_json, '$.destination_details.mx_bank_transfer.reference_status') AS dest_mx_bank_ref_status,

        JSON_VALUE(data_json, '$.destination_details.th_bank_transfer.reference') AS dest_th_bank_ref,
        JSON_VALUE(data_json, '$.destination_details.th_bank_transfer.reference_status') AS dest_th_bank_ref_status,

        JSON_VALUE(data_json, '$.destination_details.us_bank_transfer.reference') AS dest_us_bank_ref,
        JSON_VALUE(data_json, '$.destination_details.us_bank_transfer.reference_status') AS dest_us_bank_ref_status,

        -- ===== DESTINATION DETAILS - MULTIBANCO / MB WAY / P24 =====
        JSON_VALUE(data_json, '$.destination_details.multibanco.reference') AS dest_multibanco_ref,
        JSON_VALUE(data_json, '$.destination_details.multibanco.reference_status') AS dest_multibanco_ref_status,

        JSON_VALUE(data_json, '$.destination_details.mb_way.reference') AS dest_mb_way_ref,
        JSON_VALUE(data_json, '$.destination_details.mb_way.reference_status') AS dest_mb_way_ref_status,

        JSON_VALUE(data_json, '$.destination_details.p24.reference') AS dest_p24_ref,
        JSON_VALUE(data_json, '$.destination_details.p24.reference_status') AS dest_p24_ref_status,

        -- ===== DESTINATION DETAILS - SWISH =====
        JSON_VALUE(data_json, '$.destination_details.swish.reference') AS dest_swish_ref,
        JSON_VALUE(data_json, '$.destination_details.swish.reference_status') AS dest_swish_ref_status,
        JSON_VALUE(data_json, '$.destination_details.swish.network_decline_code') AS dest_swish_decline_code,

        -- ===== DESTINATION DETAILS - PAYPAL =====
        JSON_VALUE(data_json, '$.destination_details.paypal.network_decline_code') AS dest_paypal_decline_code,

        -- ===== NEXT ACTION =====
        JSON_VALUE(data_json, '$.next_action.type') AS next_action_type,
        JSON_VALUE(data_json, '$.next_action.display_details.expires_at') AS next_action_expires_at,

        TIMESTAMP_SECONDS(
            SAFE_CAST(JSON_VALUE(data_json, '$.next_action.display_details.email_sent.email_sent_at') AS INT64)
        ) AS next_action_email_sent_at,

        JSON_VALUE(data_json, '$.next_action.display_details.email_sent.email_sent_to')
            AS next_action_email_sent_to,

        -- ===== CREATED =====
        TIMESTAMP_SECONDS(
            SAFE_CAST(JSON_VALUE(data_json, '$.created') AS INT64)
        ) AS created_at,

        raw_id

    FROM source
)

SELECT * FROM parsed