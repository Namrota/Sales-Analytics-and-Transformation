SELECT
    order_id,
    customer,
    amount,
    amount * 0.18 AS estimated_tax,
    amount * 1.18 AS total_with_tax
FROM read_csv_auto(
    '/opt/airflow/dbt/sales_dbt/seeds/sales.csv',
    header = true
)