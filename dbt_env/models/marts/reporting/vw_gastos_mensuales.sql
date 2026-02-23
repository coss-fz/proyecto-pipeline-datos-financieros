{{ config(materialized='view') }}

SELECT
    df.year AS anio,
    df.month AS mes,
    fg.country AS pais,
    fg.category,
    SUM(fg.amount_usd) AS gastos_usd
FROM {{ ref('fact_gastos') }} fg
INNER JOIN {{ ref('dim_fecha') }} df ON fg.date_key = df.date_key
GROUP BY df.year, df.month, fg.country, fg.category