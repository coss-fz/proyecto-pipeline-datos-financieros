WITH RECURSIVE date_series(date) AS (
    SELECT date('2024-01-01')
    UNION ALL
    SELECT date(date, '+1 day')
    FROM date_series
    WHERE date < '2024-12-31'
)
SELECT
    date AS date_day,
    CAST(strftime('%Y%m%d', date) AS INT) AS date_key,
    CAST(strftime('%Y', date) AS INT) AS year,
    CAST('Q' || ((CAST(strftime('%m', date) AS INT) - 1) / 3 + 1) AS TEXT) AS quarter,
    CAST(strftime('%m', date) AS INT) AS month,
    CAST(strftime('%d', date) AS INT) AS day,
    CAST(strftime('%W', date) AS INT) AS week_of_year,
    CASE
        WHEN strftime('%w', date) IN ('0', '6') THEN CAST(1 AS BOOLEAN) ELSE CAST(0 AS BOOLEAN)
    END AS is_weekend
FROM date_series