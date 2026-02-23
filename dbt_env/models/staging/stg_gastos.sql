SELECT
    CAST(expense_id AS TEXT) AS expense_id,
    date,
    CAST(provider AS TEXT) AS provider,
    CAST(UPPER(TRIM(category)) AS TEXT) AS category,
    CAST(amount_usd AS REAL) AS amount_usd,
    CAST(UPPER(TRIM(country)) AS TEXT) AS country
FROM {{ source('raw', 'raw_gastos') }}
WHERE expense_id IS NOT NULL
  AND amount_usd > 0