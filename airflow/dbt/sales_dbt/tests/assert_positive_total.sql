SELECT *
FROM {{ ref('example_model') }}
WHERE total_with_tax <= 0