from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt/britmart_analytics"

DBT_ENV = {
    "DBT_PROFILES_DIR": DBT_PROJECT_DIR,
    "DBT_LOG_PATH": "/tmp/britmart-dbt-logs",
    "DBT_TARGET_PATH": "/tmp/britmart-dbt-target",
}

with DAG(
    dag_id="britmart_airflow_dbt_pipeline",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["dbt", "duckdb", "britmart"],
) as dag:

    check_dbt_connection = BashOperator(
        task_id="check_dbt_connection",
        bash_command=f"dbt debug --project-dir {DBT_PROJECT_DIR}",
        env=DBT_ENV,
        append_env=True,
    )

    run_dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command=(
            f"dbt run --project-dir {DBT_PROJECT_DIR} "
            "--select stg_orders"
        ),
        env=DBT_ENV,
        append_env=True,
    )

    test_dbt_models = BashOperator(
        task_id="test_dbt_models",
        bash_command=(
            f"dbt test --project-dir {DBT_PROJECT_DIR} "
            "--select stg_orders"
        ),
        env=DBT_ENV,
        append_env=True,
    )

    check_dbt_connection >> run_dbt_models >> test_dbt_models