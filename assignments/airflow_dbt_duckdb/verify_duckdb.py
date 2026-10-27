
import duckdb

DB_PATH = "/opt/airflow/dbt/sales_dbt/sales.duckdb"

def verify_sales_table():
    con = duckdb.connect(DB_PATH, read_only=True)

    try:
        query = """
        SELECT
            order_id,
            customer,
            amount,
            estimated_tax,
            total_with_tax
        FROM main.example_model
        ORDER BY order_id
        """

        df = con.execute(query).fetchdf()

        print("Sales transformation results:")
        print(df.to_string(index=False))
        print(f"\nTotal records: {len(df)}")

    finally:
        con.close()


if __name__ == "__main__":
    verify_sales_table()
