from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="child_dag",
    start_date=datetime(2025, 6, 25),
    catchup=False,
    schedule=None  
)

def triggered_data(**context):
    conf = context.get("dag_run").conf or {}

    print("Child DAG started")
    print(f"Triggered by: {conf.get('triggered_by', 'unknown')}")
    print(f"Run date: {conf.get('parent_run_date', 'unknown')}")
    print(f"Received result: {conf.get('processed_result', 'No result')}")

    return "completed"

child_task = PythonOperator(
    task_id="child_task",
    python_callable=triggered_data,
    dag=dag
)