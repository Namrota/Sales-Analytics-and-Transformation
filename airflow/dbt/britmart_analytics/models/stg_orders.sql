SELECT
    CAST(order_id AS INTEGER) AS order_id,
    CAST(customer_id AS INTEGER) AS customer_id,
    CAST(order_date AS DATE) AS order_date,
    CAST(quantity AS INTEGER) AS quantity,
    CAST(unit_price AS DECIMAL(12,2)) AS unit_price,
    CAST(quantity * unit_price AS DECIMAL(12,2)) AS sales_amount
FROM {{ ref('orders') }}
WHERE quantity > 0
  AND unit_price >= 0