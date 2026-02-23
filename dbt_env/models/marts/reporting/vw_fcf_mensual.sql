{{ config(materialized='view') }}

SELECT
    COALESCE(i.anio, g.anio) AS anio,
    COALESCE(i.mes, g.mes) AS mes,
    COALESCE(i.ingresos_usd, 0) AS ingresos_usd,
    COALESCE(g.gastos_usd, 0) AS gastos_usd,
    COALESCE(i.ingresos_usd, 0) - COALESCE(g.gastos_usd, 0) AS fcf_usd
FROM (
    SELECT anio, mes, SUM(ingresos_usd) AS ingresos_usd
    FROM {{ ref('vw_ingresos_mensuales') }} GROUP BY anio, mes
) i
FULL JOIN (
    SELECT anio, mes, SUM(gastos_usd) AS gastos_usd
    FROM {{ ref('vw_gastos_mensuales') }} GROUP BY anio, mes
) g ON i.anio = g.anio AND i.mes = g.mes