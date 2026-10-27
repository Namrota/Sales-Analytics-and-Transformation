from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt/sales_dbt"

DBT_ENV = {
    "DBT_PROFILES_DIR": DBT_PROJECT_DIR,
    "DBT_LOG_PATH": "/tmp/sales-dbt-logs",
    "DBT_TARGET_PATH": "/tmp/sales-dbt-target",
}

with DAG(
    dag_id="sales_dbt_pipeline",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["dbt", "duckdb", "sales_assignment"],
) as dag:

    check_dbt = BashOperator(
        task_id="check_dbt",
        bash_command=f"dbt debug --project-dir {DBT_PROJECT_DIR}",
        env=DBT_ENV,
        append_env=True,
    )

    build_models_and_test = BashOperator(
        task_id="build_models_and_test",
        bash_command=(
            f"dbt build --project-dir {DBT_PROJECT_DIR} "
            "--select example_model"
        ),
        env=DBT_ENV,
        append_env=True,
    )

    check_dbt >> build_models_and_test