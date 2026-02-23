{{ config(materialized='view') }}

SELECT
    df.year AS anio,
    df.month AS mes,
    dc.country AS pais,
    fs.plan,
    COUNT(DISTINCT fs.subscription_key) AS total_suscripciones,
    SUM(fs.monthly_price_usd) AS mrr_usd
FROM {{ ref('fact_suscripciones') }} fs
INNER JOIN {{ ref('dim_clientes') }} dc ON fs.customer_key = dc.customer_key
INNER JOIN {{ ref('dim_fecha') }} df
       ON  df.date_key >= fs.start_date_key
       AND df.date_key < COALESCE(fs.end_date_key, 99991231)
       AND df.day = 1
WHERE fs.status = 'ACTIVE'
GROUP BY df.year, df.month, dc.country, fs.plan