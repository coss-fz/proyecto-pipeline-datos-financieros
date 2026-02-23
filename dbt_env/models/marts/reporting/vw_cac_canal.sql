{{ config(materialized='view') }}

WITH marketing_anual AS (
    SELECT
        anio,
        SUM(gastos_usd) AS gasto_marketing_usd
    FROM {{ ref('vw_gastos_mensuales') }}
    WHERE category = 'MARKETING'
    GROUP BY anio
), clientes_anual AS (
    SELECT
        anio,
        acquisition_channel,
        SUM(nuevos_clientes) AS nuevos_clientes
    FROM {{ ref('vw_nuevos_clientes_trimestrales') }}
    GROUP BY anio, acquisition_channel
), conteo_canales AS (
    SELECT
        anio,
        COUNT(DISTINCT acquisition_channel) AS num_canales
    FROM clientes_anual
    GROUP BY anio
)
SELECT
    ca.anio,
    ca.acquisition_channel,
    ca.nuevos_clientes,
    ROUND(
        CASE 
            WHEN ca.nuevos_clientes = 0 THEN NULL
            ELSE (ma.gasto_marketing_usd / cc.num_canales) / ca.nuevos_clientes
        END,
    2) AS cac_usd-- CAC proporcional por canal (asume reparto uniforme del gasto)
FROM clientes_anual ca
JOIN marketing_anual ma ON ca.anio = ma.anio
JOIN conteo_canales cc ON ca.anio = cc.anio