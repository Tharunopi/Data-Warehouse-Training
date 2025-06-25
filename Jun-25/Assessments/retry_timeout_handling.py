import time, logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

args = {
    "retries": 5,
    "retry_delay": timedelta(seconds=45),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5)
}

dag = DAG(
    dag_id="retry_timeout_handling",
    schedule="@daily",
    start_date=datetime(2025, 6, 25),
    catchup=False,
    default_args=args
)

def long_work(**context):
    logging.info("Work started")
    time.sleep(60)
    logging.info("Work ended")

def success(**context):
    ti = context["ti"]
    logging.info(f"Success after: {ti.try_number} attempts")

def fail(**context):
    ti = context["ti"]
    logging.error(f"Failed after: {ti.try_number}")

work_task = PythonOperator(
    task_id="work_task",
    python_callable=long_work,
    dag=dag,
    execution_timeout=timedelta(minutes=3)
)

success_task = PythonOperator(
    task_id="success_task",
    python_callable=success,
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS
)

fail_task = PythonOperator(
    task_id="fail_task",
    python_callable=fail,
    dag=dag,
    trigger_rule=TriggerRule.ALL_FAILED
)

work_task >> [success_task, fail_task]