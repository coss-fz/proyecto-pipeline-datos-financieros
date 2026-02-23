{{ config(materialized='view') }}

SELECT
    df.year AS anio,
    df.month AS mes,
    ft.country AS pais,
    COUNT(ft.transaction_key) AS total_transacciones,
    SUM(ft.total_usd) AS ingresos_usd
FROM {{ ref('fact_transacciones') }} ft
INNER JOIN {{ ref('dim_fecha') }} df ON ft.date_key = df.date_key
GROUP BY df.year, df.month, ft.country