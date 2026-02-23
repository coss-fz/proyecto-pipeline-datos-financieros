WITH RECURSIVE date_series(date) AS (
    SELECT date('2024-01-01')
    UNION ALL
    SELECT date(date, '+1 day')
    FROM date_series
    WHERE date < '2024-12-31'
)
SELECT
    date AS date_day,
    CAST(strftime('%Y%m%d', date) AS INTEGER) AS date_key,
    strftime('%Y', date) AS year,
    strftime('%m', date) AS month,
    strftime('%d', date) AS day,
    strftime('%W', date) AS week_of_year,
    CASE WHEN strftime('%w', date) IN ('0', '6') THEN 1 ELSE 0 END AS is_weekend
FROM date_series