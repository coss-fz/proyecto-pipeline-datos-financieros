{{ config(materialized='view') }}

SELECT
    df.year AS anio,
    df.quarter AS trimestre,
    dc.country AS pais,
    dc.acquisition_channel,
    COUNT(dc.customer_key) AS nuevos_clientes
FROM {{ ref('dim_clientes') }} dc
INNER JOIN {{ ref('dim_fecha') }} df ON dc.registration_date_key = df.date_key
GROUP BY df.year, df.month, dc.country, dc.acquisition_channel