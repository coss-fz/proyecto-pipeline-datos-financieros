SELECT
    CAST(payment_id AS TEXT) AS payment_id,
    CAST(transaction_id AS TEXT) AS transaction_id,
    payment_date,
    CAST(UPPER(TRIM(method)) AS TEXT) AS method,
    CAST(amount_usd AS REAL) AS amount_usd
FROM {{ source('raw', 'raw_pagos') }}
WHERE payment_id IS NOT NULL
  AND amount_usd > 0