import random
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

def simpleTask(**context):
    ti = context["ti"]
    a, b = random.random() * 100, random.random() * 100
    ti.xcom_push(key="processed_data", value=a+b)
    print(f"Performing a addittion {a} + {b} = {a + b}")
    return "task_done"

dag = DAG(
    dag_id="parent_dag",
    schedule="@daily",
    start_date=datetime(2025, 6, 25),
    catchup=False
)

simple_task = PythonOperator(
    task_id="simple_task",
    python_callable=simpleTask,
    dag=dag
)

trigger_task = TriggerDagRunOperator(
    task_id="trigger_task",
    trigger_dag_id="child_dag", 
    conf={
        "parent_run_date": "{{ ds }}",
        "triggered_by": "parent_dag",
        "processed_result": "{{ ti.xcom_pull(task_ids='simple_task', key='processed_data') }}"
    },
    dag=dag
)

simple_task >> trigger_task