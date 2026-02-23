SELECT
    subscription_id AS subscription_key,
    customer_id AS customer_key,
    CAST(strftime('%Y%m%d', start_date) AS INTEGER) AS start_date_key,
    plan,
    status,
    monthly_price_usd,
    duration_days
FROM {{ ref('stg_suscripciones') }}