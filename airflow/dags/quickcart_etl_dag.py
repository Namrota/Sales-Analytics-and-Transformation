import logging
from datetime import timedelta

import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.smtp.notifications.smtp import send_smtp_notification

import os
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

logger = logging.getLogger(__name__)

RAW_PATH = "/opt/airflow/data/quickcart/raw"

def confirm_files():
    logger.info("All four QuickCart CSV files are available!")
    logger.info("Ready to begin extraction and validation.")

def extract_csv_files():
    raw_path = Path("/opt/airflow/data/quickcart/raw")
    processed_path = Path("/opt/airflow/data/quickcart/processed")

    processed_path.mkdir(parents=True, exist_ok=True)

    files = [
        "customers.csv",
        "products.csv",
        "orders.csv",
        "clickstream.csv"
    ]

    for filename in files:
        source_file = raw_path / filename
        destination_file = processed_path / filename

        logger.info("Extracting file: %s", filename)

        df = pd.read_csv(source_file)

        logger.info(
            "%s: %d rows, %d columns",
            filename,
            df.shape[0],
            df.shape[1]
        )

        df.to_csv(destination_file, index=False)

        logger.info("Saved extracted file: %s", destination_file)


def validate_csv_files():
    processed_path = Path("/opt/airflow/data/quickcart/processed")

    primary_keys = {
        "customers.csv": "customer_id",
        "products.csv": "product_id",
        "orders.csv": "order_id",
        "clickstream.csv": "event_id"
    }

    validation_failed = False

    for filename, primary_key in primary_keys.items():
        file_path = processed_path / filename
        df = pd.read_csv(file_path)
        logger.info("Validating %s", filename)

        # Check 1: Primary key must exist
        if primary_key not in df.columns:
            logger.error("%s: Missing primary key column %s",
                         filename, primary_key)
            validation_failed = True
            continue

        # Check 2: Primary key cannot contain NULL
        null_count = df[primary_key].isna().sum()

        if null_count > 0:
            logger.error(
                "%s: %d NULL primary keys",
                filename, null_count
            )
            validation_failed = True

        # Check 3: Detect duplicate primary keys
        duplicate_count = df.duplicated(
            subset=[primary_key], keep=False
        ).sum()

        if duplicate_count > 0:
            if filename == "clickstream.csv":
                logger.warning(
                    "%s: %d rows have conflicting %s values. "
                    "These records will be handled by clean_clickstream.",
                    filename,
                    duplicate_count,
                    primary_key
            )

            else:
                logger.error(
                "%s: %d rows have duplicate %s values",
                filename,
                duplicate_count,
                primary_key
            )
                validation_failed = True

        # Check 4: Orders must have positive quantities
        if filename == "orders.csv":
            if "quantity" not in df.columns:
                logger.error("orders.csv: Missing quantity column")
                validation_failed = True
            else:
                quantity = pd.to_numeric(
                    df["quantity"], errors="coerce"
                )
                invalid_count = (
                    quantity.isna() | (quantity <= 0)
                ).sum()

                if invalid_count:
                    logger.error(
                        "orders.csv: %d invalid quantities",
                        invalid_count
                    )
                    validation_failed = True

        logger.info(
            "Completed validation: %s (%d rows)",
            filename, len(df)
        )

    if validation_failed:
        raise ValueError(
            "QuickCart validation failed. Review task logs."
        )

    logger.info("All QuickCart datasets passed validation.")


def clean_clickstream():

    base_path = Path("/opt/airflow/data/quickcart")

    source = base_path / "processed" / "clickstream.csv"
    customers_path = base_path / "processed" / "customers.csv"

    valid_dir = base_path / "validated"
    rejected_dir = base_path / "rejected"

    # Create output directories
    valid_dir.mkdir(parents=True, exist_ok=True)
    rejected_dir.mkdir(parents=True, exist_ok=True)

    #read_csv file with pandas:
    df = pd.read_csv(source)
    customers = pd.read_csv(customers_path)

    logger.info("Original clickstream rows: %d", len(df))

    # 1. Identify conflicting event IDs
    duplicate_mask = df.duplicated(
        subset=["event_id"],
        keep=False
    )

    conflicting_events = df.loc[duplicate_mask].copy()
    valid_events = df.loc[~duplicate_mask].copy()

    logger.warning(
        "Rows with conflicting event IDs: %d",
        len(conflicting_events)
    )

    # 2. Normalize event types
    valid_events["event_type"] = (
        valid_events["event_type"]
        .astype("string")
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .replace({"pageview": "page_view"})
    )

    # 3. Normalize device types
    valid_events["device_type"] = (
        valid_events["device_type"]
        .astype("string")
        .str.strip()
        .str.lower()
        .replace("", pd.NA)
        .fillna("unknown")
    )

    # removing rows with null page_url:
    invalid_page_mask = (
        valid_events["page_url"].isna()
        | valid_events["page_url"]
            .astype("string")
            .str.strip()
            .eq("")
    ).fillna(True)

    invalid_page_rows = valid_events.loc[
        invalid_page_mask
    ].copy()

    valid_events = valid_events.loc[
        ~invalid_page_mask
    ].copy()

    invalid_page_rows["rejection_reason"] = "missing_page_url"

    logger.warning(
        "Rows with missing page_url: %d",
        len(invalid_page_rows)
    )

    # 4. Parse mixed timestamp formats
    valid_events["event_timestamp"] = pd.to_datetime(
        valid_events["event_timestamp"],
        format="mixed",
        dayfirst=True,
        utc=True,
        errors="coerce"
    )

    # 5. Quarantine invalid timestamps
    invalid_timestamp_mask = (
        valid_events["event_timestamp"].isna()
    )

    invalid_timestamps = valid_events.loc[
        invalid_timestamp_mask
    ].copy()

    valid_events = valid_events.loc[
        ~invalid_timestamp_mask
    ].copy()

    # 6. Convert customer IDs to numeric
    customer_ids = pd.to_numeric(
        valid_events["customer_id"],
        errors="coerce"
    )

    # Detect malformed IDs, but not missing IDs
    invalid_customer_format = (
        (
            valid_events["customer_id"].notna()
            & customer_ids.isna()
        )
        |
        (
            customer_ids.notna()
            & (customer_ids % 1 != 0)
        )
    ).fillna(False)

    invalid_customer_rows = valid_events.loc[
        invalid_customer_format
    ].copy()

    valid_events = valid_events.loc[
        ~invalid_customer_format
    ].copy()

    # Convert IDs to nullable integer
    valid_events["customer_id"] = (
        customer_ids.loc[valid_events.index].astype("Int64")
    )

    logger.info(
        "Anonymous clickstream events: %d",
        valid_events["customer_id"].isna().sum()
    )

    # 7. Check customer references
    known_customers = pd.to_numeric(
        customers["customer_id"],
        errors="raise"
    ).astype("Int64")

    invalid_reference_mask = (
        valid_events["customer_id"].notna()
        & ~valid_events["customer_id"].isin(known_customers)
    )

    invalid_references = valid_events.loc[
        invalid_reference_mask
    ].copy()

    valid_events = valid_events.loc[
        ~invalid_reference_mask
    ].copy()

    # adding a reason column for proper validation:
    conflicting_events["rejection_reason"] = "duplicate_event_id"
    invalid_page_rows["rejection_reason"] = "empty page URL"
    invalid_timestamps["rejection_reason"] = "invalid_timestamp"
    invalid_customer_rows["rejection_reason"] = "invalid_customer_id"
    invalid_references["rejection_reason"] = "unknown_customer_id"
    # 8. Combine all rejected categories ONCE
    rejected_events = pd.concat(
        [
            conflicting_events,
            invalid_page_rows,
            invalid_timestamps,
            invalid_customer_rows,
            invalid_references
        ],
        ignore_index=True
    )

    # 9. Save the results
    valid_events.to_csv(
        valid_dir / "valid_clickstream.csv",
        index=False
    )

    rejected_events.to_csv(
        rejected_dir / "rejected_clickstream.csv",
        index=False
    )

    # 10. Log data-quality metrics
    logger.info("Valid clickstream rows: %d", len(valid_events))
    logger.warning(
        "Rejected clickstream rows: %d",
        len(rejected_events)
    )

    logger.info(
        "Rejected breakdown - duplicate IDs: %d, "
        "missing page URLs: %d, "
        "invalid timestamps: %d, "
        "malformed customer IDs: %d, "
        "invalid customer references: %d",
        len(conflicting_events),
        len(invalid_page_rows),
        len(invalid_timestamps),
        len(invalid_customer_rows),
        len(invalid_references)
    )

    logger.info(
        "Row reconciliation: original=%d, valid=%d, rejected=%d",
        len(df),
        len(valid_events),
        len(rejected_events)
    )

    if len(df) != len(valid_events) + len(rejected_events):
        raise ValueError("Row reconciliation failed")


# SQL staging phase:

def get_mysql_engine():
    """Create a connection engine for QuickCart MySQL."""

    db_url = URL.create(
        drivername="mysql+pymysql",
        username=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASSWORD"],
        host=os.environ["MYSQL_HOST"],
        port=int(os.getenv("MYSQL_PORT", "3306")),
        database=os.environ["MYSQL_DATABASE"],
    )

    return create_engine(db_url, pool_pre_ping=True)


def load_raw_staging(batch_id):
    """Load all original QuickCart CSVs into MySQL staging."""

    raw_path = Path("/opt/airflow/data/quickcart/raw")

    files = {
        "customers.csv": "stg_customers",
        "products.csv": "stg_products",
        "orders.csv": "stg_orders",
        "clickstream.csv": "stg_clickstream",
    }

    expected_columns = {
        "customers.csv": [
            "customer_id", "customer_name", "email",
            "city", "signup_date"
        ],
        "products.csv": [
            "product_id", "product_name", "category",
            "supplier_id", "cost_price", "selling_price"
        ],
        "orders.csv": [
            "order_id", "customer_id", "product_id",
            "order_date", "quantity", "unit_price",
            "payment_status"
        ],
        "clickstream.csv": [
            "event_id", "customer_id", "event_type",
            "page_url", "event_timestamp", "device_type"
        ],
    }

    engine = get_mysql_engine()

    try:
        # One transaction for the full staging load
        with engine.begin() as conn:

            for filename, table_name in files.items():

                file_path = raw_path / filename

                logger.info("Loading file: %s", filename)

                # Preserve CSV field values as strings
                df = pd.read_csv(
                    file_path,
                    dtype=str,
                    keep_default_na=False
                )

                # Validate expected structure
                if list(df.columns) != expected_columns[filename]:
                    raise ValueError(
                        f"Unexpected columns in {filename}: "
                        f"{list(df.columns)}"
                    )

                # Add technical metadata
                # range starts at 2 to account for header row in CSV
                # range stop is len(df) + 2 to include the last row
                # batch_id is a string representing the execution date (YYYY-MM-DD)
                df.insert(0, "source_row_number", range(2, len(df) + 2))
                df.insert(0, "batch_id", batch_id)

                # Make reruns safe for the same batch
                conn.execute(
                    text(
                        f"DELETE FROM {table_name} "
                        "WHERE batch_id = :batch_id"
                    ),
                    {"batch_id": batch_id},
                )

                # Insert raw records
                df.to_sql(
                    name=table_name,
                    con=conn,
                    if_exists="append",
                    index=False,
                    chunksize=500,
                )

                logger.info(
                    "Staged %d rows in %s for batch %s",
                    len(df),
                    table_name,
                    batch_id
                )

        logger.info("Raw staging load completed successfully")

    finally:
        engine.dispose()

def transform_daily_sales(batch_id):
    engine = get_mysql_engine()

    try:
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO daily_sales_summary
                        (sales_date, total_orders, total_revenue)
                    SELECT
                        STR_TO_DATE(order_date, '%Y-%m-%d'),
                        COUNT(*),
                        SUM(
                            CAST(quantity AS SIGNED)
                            * CAST(unit_price AS DECIMAL(12,2))
                        )
                    FROM stg_orders
                    WHERE batch_id = :batch_id
                      AND LOWER(TRIM(payment_status)) = 'paid'
                    GROUP BY STR_TO_DATE(order_date, '%Y-%m-%d')

                    ON DUPLICATE KEY UPDATE
                        total_orders = VALUES(total_orders),
                        total_revenue = VALUES(total_revenue)
                """),
                {"batch_id": batch_id}
            )

        logger.info(
            "Daily sales transformation completed for batch %s",
            batch_id
        )

    finally:
        engine.dispose()

failure_alert = send_smtp_notification(
    smtp_conn_id="smtp_default",
    from_email=os.environ["GMAIL_ADDRESS"],
    to=os.environ["GMAIL_ADDRESS"],
    subject="QuickCart ETL failure - {{ ti.task_id }}",
    html_content="""
        <h3>QuickCart ETL Task Failed</h3>
        <p><b>DAG:</b> {{ dag.dag_id }}</p>
        <p><b>Task:</b> {{ ti.task_id }}</p>
        <p><b>Run ID:</b> {{ run_id }}</p>
        <p>Please review the task logs in Airflow.</p>
    """,
)

# DAG section

default_args = {
    "owner": "quickcart_team",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": failure_alert,
}

with DAG(
    dag_id="quickcart_daily_etl",
    description="QuickCart CSV ingestion into MySQL",
    start_date=pendulum.datetime(2026, 10, 23, tz="Europe/Berlin"),
    schedule="0 2 * * *",
    catchup=False,
    default_args=default_args,
    tags=["quickcart", "etl"],
) as dag:

    wait_for_customers = FileSensor(
        task_id="wait_for_customers",
        filepath=f"{RAW_PATH}/customers.csv",
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=300,
        mode="reschedule",
    )

    wait_for_products = FileSensor(
        task_id="wait_for_products",
        filepath=f"{RAW_PATH}/products.csv",
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=300,
        mode="reschedule",
    )

    wait_for_orders = FileSensor(
        task_id="wait_for_orders",
        filepath=f"{RAW_PATH}/orders.csv",
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=300,
        mode="reschedule",
    )

    wait_for_clickstream = FileSensor(
        task_id="wait_for_clickstream",
        filepath=f"{RAW_PATH}/clickstream.csv",
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=300,
        mode="reschedule",
    )

    confirm = PythonOperator(
        task_id="confirm_files",
        python_callable=confirm_files,
    )

    extract = PythonOperator(
    task_id="extract_csv_files",
    python_callable=extract_csv_files,
    )

    validate = PythonOperator(
    task_id="validate_csv_files",
    python_callable=validate_csv_files,
    )

    clean_events = PythonOperator(
    task_id="clean_clickstream",
    python_callable=clean_clickstream,
    )

    load_staging = PythonOperator(
    task_id="load_raw_staging",
    python_callable=load_raw_staging,
    op_kwargs={"batch_id": "{{ ds }}"},
    )

    transform = PythonOperator(
    task_id="transform_daily_sales",
    python_callable=transform_daily_sales,
    op_kwargs={"batch_id": "{{ ds }}"},
    )

# tasks flow:
    [
        wait_for_customers,
        wait_for_products,
        wait_for_orders,
        wait_for_clickstream,
    ] >> confirm >> extract >> validate >> load_staging >> clean_events >> transform