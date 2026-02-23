SELECT
    CAST(customer_id AS TEXT) AS customer_id,
    CAST(UPPER(TRIM(country)) AS TEXT) AS country,
    CAST(UPPER(TRIM(acquisition_channel)) AS TEXT) AS acquisition_channel,
    CAST(UPPER(TRIM(segment)) AS TEXT) AS segment,
    registration_date
FROM {{ source('raw', 'raw_clientes')}}