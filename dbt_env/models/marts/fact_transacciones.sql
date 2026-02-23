SELECT
    transaction_id AS transaction_key,
    customer_id AS customer_key,
    CAST(strftime('%Y%m%d', date) AS INTEGER) AS date_key,
    country,
    quantity,
    unit_price_usd,
    total_usd
FROM {{ ref('stg_transacciones') }}