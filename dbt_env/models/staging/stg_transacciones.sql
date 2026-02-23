SELECT
    CAST(transaction_id AS TEXT) AS transaction_id,
    CAST(customer_id AS TEXT) AS customer_id,
    CAST(product_id AS TEXT) AS product_id,
    date,
    CAST(UPPER(TRIM(country)) AS TEXT) AS country,
    CAST(quantity AS REAL) AS quantity,
    CAST(unit_price_usd AS REAL) AS unit_price_usd,
    CAST(total_usd AS REAL) AS total_usd
FROM {{ source('raw', 'raw_transacciones') }}
WHERE transaction_id IS NOT NULL
  AND total_usd > 0