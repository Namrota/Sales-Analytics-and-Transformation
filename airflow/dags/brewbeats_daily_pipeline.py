
import os
import subprocess
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task


DBT_PROJECT = "/opt/airflow/brewbeats_dbt"
DBT_PROFILES = "/opt/airflow/.dbt"


@dag(
    dag_id="brewbeats_daily_pipeline",
    description="Daily BrewBeats dbt and Snowflake pipeline",
    start_date=pendulum.datetime(
        2026, 11, 19, tz="Europe/Berlin"
    ),
    schedule="0 7 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["brewbeats", "dbt", "snowflake"],
)

def brewbeats_pipeline():

    @task
    def run_dbt_project():
        """Build dbt models and run data-quality tests."""

        subprocess.run(
            [
                "dbt",
                "build",
                "--project-dir", DBT_PROJECT,
                "--profiles-dir", DBT_PROFILES,
            ],
            check=True,
            env=os.environ.copy(),
        )

    @task
    def check_todays_sales():
        """Check that today's sales exist in Gold."""

        import snowflake.connector

        # Comment it to test with the last available historical date
        today = pendulum.now("Europe/Berlin").to_date_string()

        conn = snowflake.connector.connect(
            account="BWHMODU-XJ86285",
            user="NAMROTAG.STOCKS@GMAIL.COM",
            password=os.environ["SNOWFLAKE_PASSWORD"],
            warehouse="DBT_WH",
            database="BREWBEATS_DB",
            schema="DBT_DEV_GOLD",
            role="DBT_ROLE",
        )

        try:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM BREWBEATS_DB.DBT_DEV_GOLD.FCT_DAILY_REVENUE
                    WHERE SALE_DATE = %s
                    """,
                    (today,),
                )

                count = cursor.fetchone()[0]
            finally:
                cursor.close()
        finally:
            conn.close()

        print(f"Date checked: {today}")
        print(f"Gold mart rows for today: {count}")

        if count == 0:
            raise ValueError(
                f"No sales found in Gold mart for {today}"
            )

    dbt_task = run_dbt_project()
    validation_task = check_todays_sales()

    dbt_task >> validation_task


brewbeats_pipeline()
