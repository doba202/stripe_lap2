WITH source AS (
    SELECT
        id AS raw_id,
        data_json
    FROM {{ source('stripe_stg', 'stripe_raw_invoices') }}
),

lines AS (
    SELECT
        -- ===== KEYS =====
        JSON_VALUE(data_json, '$.id') AS invoice_id,
        JSON_VALUE(line, '$.id') AS line_id,

        -- ===== INFO =====
        JSON_VALUE(line, '$.description') AS description,
        JSON_VALUE(line, '$.currency') AS currency,

        -- ===== AMOUNT =====
        SAFE_CAST(JSON_VALUE(line, '$.amount') AS INT64) AS amount,
        SAFE_CAST(JSON_VALUE(line, '$.quantity') AS INT64) AS quantity,

        -- ===== PRICING =====
        JSON_VALUE(line, '$.pricing.price_details.price') AS price_id,
        JSON_VALUE(line, '$.pricing.price_details.product') AS product_id,

        -- ===== SUBSCRIPTION =====
        JSON_VALUE(line, '$.parent.subscription_item_details.subscription') AS subscription_id,
        JSON_VALUE(line, '$.parent.type') AS line_type,

        -- ===== PERIOD =====
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(line, '$.period.start') AS INT64)) AS period_start,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(line, '$.period.end') AS INT64)) AS period_end,

        -- debug
        raw_id

    FROM source,
    UNNEST(IFNULL(JSON_QUERY_ARRAY(data_json, '$.lines.data'), [])) AS line
)

SELECT * FROM lines