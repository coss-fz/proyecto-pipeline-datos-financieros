SELECT DISTINCT
    customer_id AS customer_key,
    country,
    acquisition_channel,
    segment,
    CAST(strftime('%Y%m%d', registration_date) AS INTEGER) AS registration_date_key
FROM {{ ref('stg_clientes') }}