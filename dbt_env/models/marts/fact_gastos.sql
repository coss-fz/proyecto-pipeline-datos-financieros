SELECT
    expense_id AS expense_key,
    CAST(strftime('%Y%m%d', date) AS INTEGER) AS date_key,
    provider,
    category,
    amount_usd,
    country
FROM {{ ref('stg_gastos') }}