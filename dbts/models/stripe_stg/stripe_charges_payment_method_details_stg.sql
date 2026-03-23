WITH source AS (
    SELECT
        open_id,
        data_json
    FROM {{ source('stripe_stg', 'stripe_raw_charges') }}
),

parsed AS (
    SELECT
        open_id,
        JSON_VALUE(data_json, '$.id') AS charge_id,
        JSON_VALUE(data_json, '$.payment_method_details.type') AS payment_method_type,

        -- =================================================================
        -- CARD (online) — payment_method_details.card
        -- =================================================================
        JSON_VALUE(data_json, '$.payment_method_details.card.brand') AS card_brand,
        JSON_VALUE(data_json, '$.payment_method_details.card.last4') AS card_last4,
        JSON_VALUE(data_json, '$.payment_method_details.card.funding') AS card_funding,
        JSON_VALUE(data_json, '$.payment_method_details.card.country') AS card_country,
        JSON_VALUE(data_json, '$.payment_method_details.card.network') AS card_network,
        SAFE_CAST(JSON_VALUE(data_json, '$.payment_method_details.card.exp_month') AS INT64) AS card_exp_month,
        SAFE_CAST(JSON_VALUE(data_json, '$.payment_method_details.card.exp_year') AS INT64) AS card_exp_year,
        JSON_VALUE(data_json, '$.payment_method_details.card.fingerprint') AS card_fingerprint,
        JSON_VALUE(data_json, '$.payment_method_details.card.mandate') AS card_mandate,
        SAFE_CAST(JSON_VALUE(data_json, '$.payment_method_details.card.amount_authorized') AS INT64) AS card_amount_authorized,
        JSON_VALUE(data_json, '$.payment_method_details.card.authorization_code') AS card_authorization_code,
        JSON_VALUE(data_json, '$.payment_method_details.card.network_transaction_id') AS card_network_transaction_id,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.payment_method_details.card.capture_before') AS INT64)) AS card_capture_before,
        JSON_VALUE(data_json, '$.payment_method_details.card.regulated_status') AS card_regulated_status,

        -- card checks
        JSON_VALUE(data_json, '$.payment_method_details.card.checks.cvc_check') AS card_cvc_check,
        JSON_VALUE(data_json, '$.payment_method_details.card.checks.address_line1_check') AS card_address_line1_check,
        JSON_VALUE(data_json, '$.payment_method_details.card.checks.address_postal_code_check') AS card_postal_check,

        -- card 3D Secure
        JSON_VALUE(data_json, '$.payment_method_details.card.three_d_secure.result') AS card_3ds_result,
        JSON_VALUE(data_json, '$.payment_method_details.card.three_d_secure.result_reason') AS card_3ds_result_reason,
        JSON_VALUE(data_json, '$.payment_method_details.card.three_d_secure.version') AS card_3ds_version,
        JSON_VALUE(data_json, '$.payment_method_details.card.three_d_secure.authentication_flow') AS card_3ds_auth_flow,
        JSON_VALUE(data_json, '$.payment_method_details.card.three_d_secure.electronic_commerce_indicator') AS card_3ds_eci,
        JSON_VALUE(data_json, '$.payment_method_details.card.three_d_secure.exemption_indicator') AS card_3ds_exemption,
        CAST(JSON_VALUE(data_json, '$.payment_method_details.card.three_d_secure.exemption_indicator_applied') AS BOOL) AS card_3ds_exemption_applied,
        JSON_VALUE(data_json, '$.payment_method_details.card.three_d_secure.transaction_id') AS card_3ds_transaction_id,

        -- card wallet
        JSON_VALUE(data_json, '$.payment_method_details.card.wallet.type') AS card_wallet_type,
        JSON_VALUE(data_json, '$.payment_method_details.card.wallet.dynamic_last4') AS card_wallet_dynamic_last4,

        -- card features
        JSON_VALUE(data_json, '$.payment_method_details.card.extended_authorization.status') AS card_extended_auth_status,
        JSON_VALUE(data_json, '$.payment_method_details.card.incremental_authorization.status') AS card_incremental_auth_status,
        JSON_VALUE(data_json, '$.payment_method_details.card.multicapture.status') AS card_multicapture_status,
        JSON_VALUE(data_json, '$.payment_method_details.card.overcapture.status') AS card_overcapture_status,
        SAFE_CAST(JSON_VALUE(data_json, '$.payment_method_details.card.overcapture.maximum_amount_capturable') AS INT64) AS card_overcapture_max_amount,
        CAST(JSON_VALUE(data_json, '$.payment_method_details.card.network_token.used') AS BOOL) AS card_network_token_used,

        -- card installments
        SAFE_CAST(JSON_VALUE(data_json, '$.payment_method_details.card.installments.plan.count') AS INT64) AS card_installments_count,
        JSON_VALUE(data_json, '$.payment_method_details.card.installments.plan.interval') AS card_installments_interval,
        JSON_VALUE(data_json, '$.payment_method_details.card.installments.plan.type') AS card_installments_type,

        -- =================================================================
        -- CARD PRESENT (terminal) — payment_method_details.card_present
        -- =================================================================
        SAFE_CAST(JSON_VALUE(data_json, '$.payment_method_details.card_present.amount_authorized') AS INT64) AS cp_amount_authorized,
        JSON_VALUE(data_json, '$.payment_method_details.card_present.brand') AS cp_brand,
        JSON_VALUE(data_json, '$.payment_method_details.card_present.brand_product') AS cp_brand_product,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.payment_method_details.card_present.capture_before') AS INT64)) AS cp_capture_before,
        JSON_VALUE(data_json, '$.payment_method_details.card_present.cardholder_name') AS cp_cardholder_name,
        JSON_VALUE(data_json, '$.payment_method_details.card_present.country') AS cp_country,
        JSON_VALUE(data_json, '$.payment_method_details.card_present.description') AS cp_description,
        JSON_VALUE(data_json, '$.payment_method_details.card_present.emv_auth_data') AS cp_emv_auth_data,
        SAFE_CAST(JSON_VALUE(data_json, '$.payment_method_details.card_present.exp_month') AS INT64) AS cp_exp_month,
        SAFE_CAST(JSON_VALUE(data_json, '$.payment_method_details.card_present.exp_year') AS INT64) AS cp_exp_year,
        JSON_VALUE(data_json, '$.payment_method_details.card_present.fingerprint') AS cp_fingerprint,
        JSON_VALUE(data_json, '$.payment_method_details.card_present.funding') AS cp_funding,
        JSON_VALUE(data_json, '$.payment_method_details.card_present.generated_card') AS cp_generated_card,
        CAST(JSON_VALUE(data_json, '$.payment_method_details.card_present.incremental_authorization_supported') AS BOOL) AS cp_incremental_auth_supported,
        JSON_VALUE(data_json, '$.payment_method_details.card_present.issuer') AS cp_issuer,
        JSON_VALUE(data_json, '$.payment_method_details.card_present.last4') AS cp_last4,
        JSON_VALUE(data_json, '$.payment_method_details.card_present.location') AS cp_location,
        JSON_VALUE(data_json, '$.payment_method_details.card_present.network') AS cp_network,
        JSON_VALUE(data_json, '$.payment_method_details.card_present.network_transaction_id') AS cp_network_transaction_id,
        CAST(JSON_VALUE(data_json, '$.payment_method_details.card_present.overcapture_supported') AS BOOL) AS cp_overcapture_supported,
        JSON_VALUE(data_json, '$.payment_method_details.card_present.read_method') AS cp_read_method,
        JSON_VALUE(data_json, '$.payment_method_details.card_present.reader') AS cp_reader,
        JSON_VALUE(data_json, '$.payment_method_details.card_present.wallet.type') AS cp_wallet_type,

        -- card present offline
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(data_json, '$.payment_method_details.card_present.offline.stored_at') AS INT64)) AS cp_offline_stored_at,
        JSON_VALUE(data_json, '$.payment_method_details.card_present.offline.type') AS cp_offline_type,

        -- card present receipt
        JSON_VALUE(data_json, '$.payment_method_details.card_present.receipt.account_type') AS cp_receipt_account_type,
        JSON_VALUE(data_json, '$.payment_method_details.card_present.receipt.application_cryptogram') AS cp_receipt_app_cryptogram,
        JSON_VALUE(data_json, '$.payment_method_details.card_present.receipt.application_preferred_name') AS cp_receipt_app_preferred_name,
        JSON_VALUE(data_json, '$.payment_method_details.card_present.receipt.authorization_code') AS cp_receipt_auth_code,
        JSON_VALUE(data_json, '$.payment_method_details.card_present.receipt.authorization_response_code') AS cp_receipt_auth_response_code,
        JSON_VALUE(data_json, '$.payment_method_details.card_present.receipt.cardholder_verification_method') AS cp_receipt_verification_method,
        JSON_VALUE(data_json, '$.payment_method_details.card_present.receipt.dedicated_file_name') AS cp_receipt_dedicated_file_name,
        JSON_VALUE(data_json, '$.payment_method_details.card_present.receipt.terminal_verification_results') AS cp_receipt_terminal_results,
        JSON_VALUE(data_json, '$.payment_method_details.card_present.receipt.transaction_status_information') AS cp_receipt_txn_status_info,

        -- =================================================================
        -- INTERAC PRESENT — payment_method_details.interac_present
        -- =================================================================
        JSON_VALUE(data_json, '$.payment_method_details.interac_present.brand') AS ip_brand,
        JSON_VALUE(data_json, '$.payment_method_details.interac_present.cardholder_name') AS ip_cardholder_name,
        JSON_VALUE(data_json, '$.payment_method_details.interac_present.country') AS ip_country,
        JSON_VALUE(data_json, '$.payment_method_details.interac_present.description') AS ip_description,
        JSON_VALUE(data_json, '$.payment_method_details.interac_present.emv_auth_data') AS ip_emv_auth_data,
        SAFE_CAST(JSON_VALUE(data_json, '$.payment_method_details.interac_present.exp_month') AS INT64) AS ip_exp_month,
        SAFE_CAST(JSON_VALUE(data_json, '$.payment_method_details.interac_present.exp_year') AS INT64) AS ip_exp_year,
        JSON_VALUE(data_json, '$.payment_method_details.interac_present.fingerprint') AS ip_fingerprint,
        JSON_VALUE(data_json, '$.payment_method_details.interac_present.funding') AS ip_funding,
        JSON_VALUE(data_json, '$.payment_method_details.interac_present.generated_card') AS ip_generated_card,
        JSON_VALUE(data_json, '$.payment_method_details.interac_present.issuer') AS ip_issuer,
        JSON_VALUE(data_json, '$.payment_method_details.interac_present.last4') AS ip_last4,
        JSON_VALUE(data_json, '$.payment_method_details.interac_present.location') AS ip_location,
        JSON_VALUE(data_json, '$.payment_method_details.interac_present.network') AS ip_network,
        JSON_VALUE(data_json, '$.payment_method_details.interac_present.network_transaction_id') AS ip_network_transaction_id,
        JSON_VALUE(data_json, '$.payment_method_details.interac_present.read_method') AS ip_read_method,
        JSON_VALUE(data_json, '$.payment_method_details.interac_present.reader') AS ip_reader,

        -- interac present receipt
        JSON_VALUE(data_json, '$.payment_method_details.interac_present.receipt.account_type') AS ip_receipt_account_type,
        JSON_VALUE(data_json, '$.payment_method_details.interac_present.receipt.application_cryptogram') AS ip_receipt_app_cryptogram,
        JSON_VALUE(data_json, '$.payment_method_details.interac_present.receipt.application_preferred_name') AS ip_receipt_app_preferred_name,
        JSON_VALUE(data_json, '$.payment_method_details.interac_present.receipt.authorization_code') AS ip_receipt_auth_code,
        JSON_VALUE(data_json, '$.payment_method_details.interac_present.receipt.authorization_response_code') AS ip_receipt_auth_response_code,
        JSON_VALUE(data_json, '$.payment_method_details.interac_present.receipt.cardholder_verification_method') AS ip_receipt_verification_method,
        JSON_VALUE(data_json, '$.payment_method_details.interac_present.receipt.dedicated_file_name') AS ip_receipt_dedicated_file_name,
        JSON_VALUE(data_json, '$.payment_method_details.interac_present.receipt.terminal_verification_results') AS ip_receipt_terminal_results,
        JSON_VALUE(data_json, '$.payment_method_details.interac_present.receipt.transaction_status_information') AS ip_receipt_txn_status_info,

        -- =================================================================
        -- ACH CREDIT TRANSFER
        -- =================================================================
        JSON_VALUE(data_json, '$.payment_method_details.ach_credit_transfer.account_number') AS ach_ct_account_number,
        JSON_VALUE(data_json, '$.payment_method_details.ach_credit_transfer.bank_name') AS ach_ct_bank_name,
        JSON_VALUE(data_json, '$.payment_method_details.ach_credit_transfer.routing_number') AS ach_ct_routing_number,
        JSON_VALUE(data_json, '$.payment_method_details.ach_credit_transfer.swift_code') AS ach_ct_swift_code,

        -- =================================================================
        -- ACH DEBIT
        -- =================================================================
        JSON_VALUE(data_json, '$.payment_method_details.ach_debit.account_holder_type') AS ach_debit_account_holder_type,
        JSON_VALUE(data_json, '$.payment_method_details.ach_debit.bank_name') AS ach_debit_bank_name,
        JSON_VALUE(data_json, '$.payment_method_details.ach_debit.country') AS ach_debit_country,
        JSON_VALUE(data_json, '$.payment_method_details.ach_debit.fingerprint') AS ach_debit_fingerprint,
        JSON_VALUE(data_json, '$.payment_method_details.ach_debit.last4') AS ach_debit_last4,
        JSON_VALUE(data_json, '$.payment_method_details.ach_debit.routing_number') AS ach_debit_routing_number,

        -- =================================================================
        -- ACSS DEBIT (Canada)
        -- =================================================================
        JSON_VALUE(data_json, '$.payment_method_details.acss_debit.bank_name') AS acss_bank_name,
        JSON_VALUE(data_json, '$.payment_method_details.acss_debit.expected_debit_date') AS acss_expected_debit_date,
        JSON_VALUE(data_json, '$.payment_method_details.acss_debit.fingerprint') AS acss_fingerprint,
        JSON_VALUE(data_json, '$.payment_method_details.acss_debit.institution_number') AS acss_institution_number,
        JSON_VALUE(data_json, '$.payment_method_details.acss_debit.last4') AS acss_last4,
        JSON_VALUE(data_json, '$.payment_method_details.acss_debit.mandate') AS acss_mandate,
        JSON_VALUE(data_json, '$.payment_method_details.acss_debit.transit_number') AS acss_transit_number,

        -- =================================================================
        -- BACS DEBIT (UK)
        -- =================================================================
        JSON_VALUE(data_json, '$.payment_method_details.bacs_debit.expected_debit_date') AS bacs_expected_debit_date,
        JSON_VALUE(data_json, '$.payment_method_details.bacs_debit.fingerprint') AS bacs_fingerprint,
        JSON_VALUE(data_json, '$.payment_method_details.bacs_debit.last4') AS bacs_last4,
        JSON_VALUE(data_json, '$.payment_method_details.bacs_debit.mandate') AS bacs_mandate,
        JSON_VALUE(data_json, '$.payment_method_details.bacs_debit.sort_code') AS bacs_sort_code,

        -- =================================================================
        -- AU BECS DEBIT (Australia)
        -- =================================================================
        JSON_VALUE(data_json, '$.payment_method_details.au_becs_debit.bsb_number') AS au_becs_bsb_number,
        JSON_VALUE(data_json, '$.payment_method_details.au_becs_debit.expected_debit_date') AS au_becs_expected_debit_date,
        JSON_VALUE(data_json, '$.payment_method_details.au_becs_debit.fingerprint') AS au_becs_fingerprint,
        JSON_VALUE(data_json, '$.payment_method_details.au_becs_debit.last4') AS au_becs_last4,
        JSON_VALUE(data_json, '$.payment_method_details.au_becs_debit.mandate') AS au_becs_mandate,

        -- =================================================================
        -- SEPA DEBIT
        -- =================================================================
        JSON_VALUE(data_json, '$.payment_method_details.sepa_debit.bank_code') AS sepa_bank_code,
        JSON_VALUE(data_json, '$.payment_method_details.sepa_debit.branch_code') AS sepa_branch_code,
        JSON_VALUE(data_json, '$.payment_method_details.sepa_debit.country') AS sepa_country,
        JSON_VALUE(data_json, '$.payment_method_details.sepa_debit.expected_debit_date') AS sepa_expected_debit_date,
        JSON_VALUE(data_json, '$.payment_method_details.sepa_debit.fingerprint') AS sepa_fingerprint,
        JSON_VALUE(data_json, '$.payment_method_details.sepa_debit.last4') AS sepa_last4,
        JSON_VALUE(data_json, '$.payment_method_details.sepa_debit.mandate') AS sepa_mandate,

        -- =================================================================
        -- US BANK ACCOUNT
        -- =================================================================
        JSON_VALUE(data_json, '$.payment_method_details.us_bank_account.account_holder_type') AS us_bank_account_holder_type,
        JSON_VALUE(data_json, '$.payment_method_details.us_bank_account.account_type') AS us_bank_account_type,
        JSON_VALUE(data_json, '$.payment_method_details.us_bank_account.bank_name') AS us_bank_name,
        JSON_VALUE(data_json, '$.payment_method_details.us_bank_account.expected_debit_date') AS us_bank_expected_debit_date,
        JSON_VALUE(data_json, '$.payment_method_details.us_bank_account.fingerprint') AS us_bank_fingerprint,
        JSON_VALUE(data_json, '$.payment_method_details.us_bank_account.last4') AS us_bank_last4,
        JSON_VALUE(data_json, '$.payment_method_details.us_bank_account.mandate') AS us_bank_mandate,
        JSON_VALUE(data_json, '$.payment_method_details.us_bank_account.payment_reference') AS us_bank_payment_reference,
        JSON_VALUE(data_json, '$.payment_method_details.us_bank_account.routing_number') AS us_bank_routing_number,

        -- =================================================================
        -- NZ BANK ACCOUNT
        -- =================================================================
        JSON_VALUE(data_json, '$.payment_method_details.nz_bank_account.account_holder_name') AS nz_bank_account_holder_name,
        JSON_VALUE(data_json, '$.payment_method_details.nz_bank_account.bank_code') AS nz_bank_code,
        JSON_VALUE(data_json, '$.payment_method_details.nz_bank_account.bank_name') AS nz_bank_name,
        JSON_VALUE(data_json, '$.payment_method_details.nz_bank_account.branch_code') AS nz_bank_branch_code,
        JSON_VALUE(data_json, '$.payment_method_details.nz_bank_account.expected_debit_date') AS nz_bank_expected_debit_date,
        JSON_VALUE(data_json, '$.payment_method_details.nz_bank_account.last4') AS nz_bank_last4,
        JSON_VALUE(data_json, '$.payment_method_details.nz_bank_account.suffix') AS nz_bank_suffix,

        -- =================================================================
        -- PAYTO (Australia)
        -- =================================================================
        JSON_VALUE(data_json, '$.payment_method_details.payto.bsb_number') AS payto_bsb_number,
        JSON_VALUE(data_json, '$.payment_method_details.payto.last4') AS payto_last4,
        JSON_VALUE(data_json, '$.payment_method_details.payto.mandate') AS payto_mandate,
        JSON_VALUE(data_json, '$.payment_method_details.payto.pay_id') AS payto_pay_id,

        -- =================================================================
        -- SOFORT
        -- =================================================================
        JSON_VALUE(data_json, '$.payment_method_details.sofort.bank_code') AS sofort_bank_code,
        JSON_VALUE(data_json, '$.payment_method_details.sofort.bank_name') AS sofort_bank_name,
        JSON_VALUE(data_json, '$.payment_method_details.sofort.bic') AS sofort_bic,
        JSON_VALUE(data_json, '$.payment_method_details.sofort.country') AS sofort_country,
        JSON_VALUE(data_json, '$.payment_method_details.sofort.generated_sepa_debit') AS sofort_generated_sepa_debit,
        JSON_VALUE(data_json, '$.payment_method_details.sofort.generated_sepa_debit_mandate') AS sofort_generated_sepa_debit_mandate,
        JSON_VALUE(data_json, '$.payment_method_details.sofort.iban_last4') AS sofort_iban_last4,
        JSON_VALUE(data_json, '$.payment_method_details.sofort.preferred_language') AS sofort_preferred_language,
        JSON_VALUE(data_json, '$.payment_method_details.sofort.verified_name') AS sofort_verified_name,

        -- =================================================================
        -- BANCONTACT
        -- =================================================================
        JSON_VALUE(data_json, '$.payment_method_details.bancontact.bank_code') AS bancontact_bank_code,
        JSON_VALUE(data_json, '$.payment_method_details.bancontact.bank_name') AS bancontact_bank_name,
        JSON_VALUE(data_json, '$.payment_method_details.bancontact.bic') AS bancontact_bic,
        JSON_VALUE(data_json, '$.payment_method_details.bancontact.generated_sepa_debit') AS bancontact_generated_sepa_debit,
        JSON_VALUE(data_json, '$.payment_method_details.bancontact.generated_sepa_debit_mandate') AS bancontact_generated_sepa_debit_mandate,
        JSON_VALUE(data_json, '$.payment_method_details.bancontact.iban_last4') AS bancontact_iban_last4,
        JSON_VALUE(data_json, '$.payment_method_details.bancontact.preferred_language') AS bancontact_preferred_language,
        JSON_VALUE(data_json, '$.payment_method_details.bancontact.verified_name') AS bancontact_verified_name,

        -- =================================================================
        -- GIROPAY
        -- =================================================================
        JSON_VALUE(data_json, '$.payment_method_details.giropay.bank_code') AS giropay_bank_code,
        JSON_VALUE(data_json, '$.payment_method_details.giropay.bank_name') AS giropay_bank_name,
        JSON_VALUE(data_json, '$.payment_method_details.giropay.bic') AS giropay_bic,
        JSON_VALUE(data_json, '$.payment_method_details.giropay.verified_name') AS giropay_verified_name,

        -- =================================================================
        -- iDEAL
        -- =================================================================
        JSON_VALUE(data_json, '$.payment_method_details.ideal.bank') AS ideal_bank,
        JSON_VALUE(data_json, '$.payment_method_details.ideal.bic') AS ideal_bic,
        JSON_VALUE(data_json, '$.payment_method_details.ideal.generated_sepa_debit') AS ideal_generated_sepa_debit,
        JSON_VALUE(data_json, '$.payment_method_details.ideal.generated_sepa_debit_mandate') AS ideal_generated_sepa_debit_mandate,
        JSON_VALUE(data_json, '$.payment_method_details.ideal.iban_last4') AS ideal_iban_last4,
        JSON_VALUE(data_json, '$.payment_method_details.ideal.transaction_id') AS ideal_transaction_id,
        JSON_VALUE(data_json, '$.payment_method_details.ideal.verified_name') AS ideal_verified_name,

        -- =================================================================
        -- EPS (Austria)
        -- =================================================================
        JSON_VALUE(data_json, '$.payment_method_details.eps.bank') AS eps_bank,
        JSON_VALUE(data_json, '$.payment_method_details.eps.verified_name') AS eps_verified_name,

        -- =================================================================
        -- P24 (Poland)
        -- =================================================================
        JSON_VALUE(data_json, '$.payment_method_details.p24.bank') AS p24_bank,
        JSON_VALUE(data_json, '$.payment_method_details.p24.reference') AS p24_reference,
        JSON_VALUE(data_json, '$.payment_method_details.p24.verified_name') AS p24_verified_name,

        -- =================================================================
        -- FPX (Malaysia)
        -- =================================================================
        JSON_VALUE(data_json, '$.payment_method_details.fpx.bank') AS fpx_bank,
        JSON_VALUE(data_json, '$.payment_method_details.fpx.transaction_id') AS fpx_transaction_id,

        -- =================================================================
        -- PAYPAL
        -- =================================================================
        JSON_VALUE(data_json, '$.payment_method_details.paypal.country') AS paypal_country,
        JSON_VALUE(data_json, '$.payment_method_details.paypal.payer_email') AS paypal_payer_email,
        JSON_VALUE(data_json, '$.payment_method_details.paypal.payer_id') AS paypal_payer_id,
        JSON_VALUE(data_json, '$.payment_method_details.paypal.payer_name') AS paypal_payer_name,
        JSON_VALUE(data_json, '$.payment_method_details.paypal.seller_protection.status') AS paypal_seller_protection_status,
        JSON_VALUE(data_json, '$.payment_method_details.paypal.transaction_id') AS paypal_transaction_id,

        -- =================================================================
        -- CASHAPP
        -- =================================================================
        JSON_VALUE(data_json, '$.payment_method_details.cashapp.buyer_id') AS cashapp_buyer_id,
        JSON_VALUE(data_json, '$.payment_method_details.cashapp.cashtag') AS cashapp_cashtag,
        JSON_VALUE(data_json, '$.payment_method_details.cashapp.transaction_id') AS cashapp_transaction_id,

        -- =================================================================
        -- AMAZON PAY
        -- =================================================================
        JSON_VALUE(data_json, '$.payment_method_details.amazon_pay.transaction_id') AS amazon_pay_transaction_id,
        JSON_VALUE(data_json, '$.payment_method_details.amazon_pay.funding.type') AS amazon_pay_funding_type,
        JSON_VALUE(data_json, '$.payment_method_details.amazon_pay.funding.card.brand') AS amazon_pay_card_brand,
        JSON_VALUE(data_json, '$.payment_method_details.amazon_pay.funding.card.country') AS amazon_pay_card_country,
        SAFE_CAST(JSON_VALUE(data_json, '$.payment_method_details.amazon_pay.funding.card.exp_month') AS INT64) AS amazon_pay_card_exp_month,
        SAFE_CAST(JSON_VALUE(data_json, '$.payment_method_details.amazon_pay.funding.card.exp_year') AS INT64) AS amazon_pay_card_exp_year,
        JSON_VALUE(data_json, '$.payment_method_details.amazon_pay.funding.card.funding') AS amazon_pay_card_funding,
        JSON_VALUE(data_json, '$.payment_method_details.amazon_pay.funding.card.last4') AS amazon_pay_card_last4,

        -- =================================================================
        -- REVOLUT PAY
        -- =================================================================
        JSON_VALUE(data_json, '$.payment_method_details.revolut_pay.transaction_id') AS revolut_pay_transaction_id,
        JSON_VALUE(data_json, '$.payment_method_details.revolut_pay.funding.type') AS revolut_pay_funding_type,
        JSON_VALUE(data_json, '$.payment_method_details.revolut_pay.funding.card.brand') AS revolut_pay_card_brand,
        JSON_VALUE(data_json, '$.payment_method_details.revolut_pay.funding.card.country') AS revolut_pay_card_country,
        SAFE_CAST(JSON_VALUE(data_json, '$.payment_method_details.revolut_pay.funding.card.exp_month') AS INT64) AS revolut_pay_card_exp_month,
        SAFE_CAST(JSON_VALUE(data_json, '$.payment_method_details.revolut_pay.funding.card.exp_year') AS INT64) AS revolut_pay_card_exp_year,
        JSON_VALUE(data_json, '$.payment_method_details.revolut_pay.funding.card.funding') AS revolut_pay_card_funding,
        JSON_VALUE(data_json, '$.payment_method_details.revolut_pay.funding.card.last4') AS revolut_pay_card_last4,

        -- =================================================================
        -- MOBILEPAY
        -- =================================================================
        JSON_VALUE(data_json, '$.payment_method_details.mobilepay.card.brand') AS mobilepay_card_brand,
        JSON_VALUE(data_json, '$.payment_method_details.mobilepay.card.country') AS mobilepay_card_country,
        SAFE_CAST(JSON_VALUE(data_json, '$.payment_method_details.mobilepay.card.exp_month') AS INT64) AS mobilepay_card_exp_month,
        SAFE_CAST(JSON_VALUE(data_json, '$.payment_method_details.mobilepay.card.exp_year') AS INT64) AS mobilepay_card_exp_year,
        JSON_VALUE(data_json, '$.payment_method_details.mobilepay.card.last4') AS mobilepay_card_last4,

        -- =================================================================
        -- KLARNA
        -- =================================================================
        JSON_VALUE(data_json, '$.payment_method_details.klarna.payment_method_category') AS klarna_payment_method_category,
        JSON_VALUE(data_json, '$.payment_method_details.klarna.preferred_locale') AS klarna_preferred_locale,
        JSON_VALUE(data_json, '$.payment_method_details.klarna.payer_details.address.country') AS klarna_payer_country,

        -- =================================================================
        -- AFFIRM
        -- =================================================================
        JSON_VALUE(data_json, '$.payment_method_details.affirm.location') AS affirm_location,
        JSON_VALUE(data_json, '$.payment_method_details.affirm.reader') AS affirm_reader,
        JSON_VALUE(data_json, '$.payment_method_details.affirm.transaction_id') AS affirm_transaction_id,

        -- =================================================================
        -- AFTERPAY / CLEARPAY
        -- =================================================================
        JSON_VALUE(data_json, '$.payment_method_details.afterpay_clearpay.order_id') AS afterpay_order_id,
        JSON_VALUE(data_json, '$.payment_method_details.afterpay_clearpay.reference') AS afterpay_reference,

        -- =================================================================
        -- ALMA
        -- =================================================================
        JSON_VALUE(data_json, '$.payment_method_details.alma.transaction_id') AS alma_transaction_id,
        SAFE_CAST(JSON_VALUE(data_json, '$.payment_method_details.alma.installments.count') AS INT64) AS alma_installments_count,

        -- =================================================================
        -- ALIPAY
        -- =================================================================
        JSON_VALUE(data_json, '$.payment_method_details.alipay.buyer_id') AS alipay_buyer_id,
        JSON_VALUE(data_json, '$.payment_method_details.alipay.fingerprint') AS alipay_fingerprint,
        JSON_VALUE(data_json, '$.payment_method_details.alipay.transaction_id') AS alipay_transaction_id,

        -- =================================================================
        -- WECHAT PAY
        -- =================================================================
        JSON_VALUE(data_json, '$.payment_method_details.wechat_pay.fingerprint') AS wechat_pay_fingerprint,
        JSON_VALUE(data_json, '$.payment_method_details.wechat_pay.location') AS wechat_pay_location,
        JSON_VALUE(data_json, '$.payment_method_details.wechat_pay.reader') AS wechat_pay_reader,
        JSON_VALUE(data_json, '$.payment_method_details.wechat_pay.transaction_id') AS wechat_pay_transaction_id,

        -- =================================================================
        -- CRYPTO
        -- =================================================================
        JSON_VALUE(data_json, '$.payment_method_details.crypto.buyer_address') AS crypto_buyer_address,
        JSON_VALUE(data_json, '$.payment_method_details.crypto.network') AS crypto_network,
        JSON_VALUE(data_json, '$.payment_method_details.crypto.token_currency') AS crypto_token_currency,
        JSON_VALUE(data_json, '$.payment_method_details.crypto.transaction_hash') AS crypto_transaction_hash,

        -- =================================================================
        -- KR CARD (South Korea)
        -- =================================================================
        JSON_VALUE(data_json, '$.payment_method_details.kr_card.brand') AS kr_card_brand,
        JSON_VALUE(data_json, '$.payment_method_details.kr_card.buyer_id') AS kr_card_buyer_id,
        JSON_VALUE(data_json, '$.payment_method_details.kr_card.last4') AS kr_card_last4,
        JSON_VALUE(data_json, '$.payment_method_details.kr_card.transaction_id') AS kr_card_transaction_id,

        -- =================================================================
        -- SIMPLE METHODS (1-3 fields each)
        -- =================================================================
        -- Samsung Pay / Kakao Pay / Naver Pay / Payco
        JSON_VALUE(data_json, '$.payment_method_details.samsung_pay.buyer_id') AS samsung_pay_buyer_id,
        JSON_VALUE(data_json, '$.payment_method_details.samsung_pay.transaction_id') AS samsung_pay_transaction_id,
        JSON_VALUE(data_json, '$.payment_method_details.kakao_pay.buyer_id') AS kakao_pay_buyer_id,
        JSON_VALUE(data_json, '$.payment_method_details.kakao_pay.transaction_id') AS kakao_pay_transaction_id,
        JSON_VALUE(data_json, '$.payment_method_details.naver_pay.buyer_id') AS naver_pay_buyer_id,
        JSON_VALUE(data_json, '$.payment_method_details.naver_pay.transaction_id') AS naver_pay_transaction_id,
        JSON_VALUE(data_json, '$.payment_method_details.payco.buyer_id') AS payco_buyer_id,
        JSON_VALUE(data_json, '$.payment_method_details.payco.transaction_id') AS payco_transaction_id,

        -- Satispay / Billie / Grabpay
        JSON_VALUE(data_json, '$.payment_method_details.satispay.transaction_id') AS satispay_transaction_id,
        JSON_VALUE(data_json, '$.payment_method_details.billie.transaction_id') AS billie_transaction_id,
        JSON_VALUE(data_json, '$.payment_method_details.grabpay.transaction_id') AS grabpay_transaction_id,

        -- BLIK / Boleto
        JSON_VALUE(data_json, '$.payment_method_details.blik.buyer_id') AS blik_buyer_id,
        JSON_VALUE(data_json, '$.payment_method_details.boleto.tax_id') AS boleto_tax_id,

        -- Link
        JSON_VALUE(data_json, '$.payment_method_details.link.country') AS link_country,

        -- OXXO / Multibanco
        JSON_VALUE(data_json, '$.payment_method_details.oxxo.number') AS oxxo_number,
        JSON_VALUE(data_json, '$.payment_method_details.multibanco.entity') AS multibanco_entity,
        JSON_VALUE(data_json, '$.payment_method_details.multibanco.reference') AS multibanco_reference,

        -- Konbini
        JSON_VALUE(data_json, '$.payment_method_details.konbini.store.chain') AS konbini_store_chain,

        -- PayNow
        JSON_VALUE(data_json, '$.payment_method_details.paynow.location') AS paynow_location,
        JSON_VALUE(data_json, '$.payment_method_details.paynow.reader') AS paynow_reader,
        JSON_VALUE(data_json, '$.payment_method_details.paynow.reference') AS paynow_reference,

        -- PromptPay
        JSON_VALUE(data_json, '$.payment_method_details.promptpay.reference') AS promptpay_reference,

        -- Pix
        JSON_VALUE(data_json, '$.payment_method_details.pix.bank_transaction_id') AS pix_bank_transaction_id,

        -- Swish
        JSON_VALUE(data_json, '$.payment_method_details.swish.fingerprint') AS swish_fingerprint,
        JSON_VALUE(data_json, '$.payment_method_details.swish.payment_reference') AS swish_payment_reference,
        JSON_VALUE(data_json, '$.payment_method_details.swish.verified_phone_last4') AS swish_verified_phone_last4

    FROM source
)

SELECT * FROM parsed