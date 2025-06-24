# Simulate a flaky API call with retries and alert on final failure.

import random, logging, time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

args = {
    "owner": "airflow",
    "depends_on_past": False, 
    "retries": 5,
    "retry_delay": timedelta(seconds=60),
    "max_retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True
}

dag = DAG(
    dag_id="retry_with_alerts",
    start_date=datetime(2025, 6, 24),
    schedule="@daily",
    catchup=False,
    default_args=args
    )

def step_1():
    if random.random() >= 0.50:
        raise Exception("Unexcepted Failure!")
    logging.info("Success")

def failure_alert(context):
    logging.error("Retries exhausted")

def success_info():
    logging.info("Success run")

start = EmptyOperator(dag=dag, task_id="start")

step_1_task = PythonOperator(
    dag=dag, 
    task_id="step_1", 
    python_callable=step_1, 
    on_failure_callback=failure_alert
    )

success_task = PythonOperator(
    dag=dag,
    task_id="success_function",
    python_callable=success_info,
    trigger_rule=TriggerRule.ALL_SUCCESS
)

end = EmptyOperator(task_id="end", dag=dag)

start >> step_1_task >> success_task >> end