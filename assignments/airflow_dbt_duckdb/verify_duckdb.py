
import duckdb
from pathlib import Path

# Find this script's directory
SCRIPT_DIR = Path(__file__).resolve().parent

# Move up two directories to project root
PROJECT_ROOT = SCRIPT_DIR.parent.parent

DB_PATH = PROJECT_ROOT/"airflow"/"dbt"/"sales_dbt"/"sales.duckdb"

def verify_sales_table():
    # Check that the database actually exists
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Database not found: {DB_PATH}")

    con = duckdb.connect(str(DB_PATH), read_only=True)

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
