SELECT
    CAST(employee_id AS TEXT) AS employee_id,
    CAST(UPPER(TRIM(area)) AS TEXT) AS area,
    CAST(salary_usd AS REAL) AS salary_usd,
    CAST(UPPER(TRIM(country)) AS TEXT) AS country,
    hire_date as hire_date
FROM {{ source('raw', 'raw_empleados') }}
WHERE employee_id IS NOT NULL
  AND salary_usd > 0