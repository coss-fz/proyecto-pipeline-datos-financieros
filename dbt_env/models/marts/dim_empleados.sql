SELECT DISTINCT
    employee_id AS employee_key,
    area,
    salary_usd,
    country,
    CAST(strftime('%Y%m%d', hire_date) AS INTEGER) AS hire_date_key
FROM {{ ref('stg_empleados') }}