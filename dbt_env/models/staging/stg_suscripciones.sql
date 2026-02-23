SELECT
    CAST(subscription_id AS TEXT) AS subscription_id,
    CAST(customer_id AS TEXT) AS customer_id,
    CAST(UPPER(TRIM(plan)) AS TEXT) AS plan,
    CASE
        WHEN start_date <= end_date THEN start_date
        ELSE end_date
    END AS start_date,
    CASE
        WHEN start_date > end_date THEN start_date
        ELSE end_date
    END AS end_date,
    CAST(UPPER(TRIM(status)) AS TEXT) AS status,
    CAST(monthly_price_usd AS REAL) AS monthly_price_usd,
    CAST(ABS(julianday(end_date) - julianday(start_date)) AS REAL) AS duration_days
FROM {{ source('raw', 'raw_suscripciones') }}
WHERE subscription_id IS NOT NULL