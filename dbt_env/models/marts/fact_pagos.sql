SELECT
    payment_id AS payment_key,
    transaction_id AS transaction_key,
    CAST(strftime('%Y%m%d', payment_date) AS INTEGER) AS payment_date_key,
    method AS payment_method,
    amount_usd
FROM {{ ref('stg_pagos') }}