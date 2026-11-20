
import os
import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


def intentional_failure():
    raise ValueError(
        "This is a test failure for QuickCart Gmail alerts."
    )


with DAG(
    dag_id="test_gmail_failure",
    start_date=pendulum.datetime(
        2026, 10, 23, tz="Europe/Berlin"
    ),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "quickcart_team",
        "email": [os.environ["GMAIL_ADDRESS"]],
        "email_on_failure": True,
        "email_on_retry": False,
        "retries": 0,
    },
    tags=["quickcart", "smtp-test"],
) as dag:

    test_failure = PythonOperator(
        task_id="test_failure",
        python_callable=intentional_failure,
    )
