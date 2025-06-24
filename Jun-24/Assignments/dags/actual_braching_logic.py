import os
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime

filePath = "/opt/airflow/dags/customers.csv"
bytesThreshold = 500 * 1024

dag = DAG(
    dag_id="branching_logic",
    start_date=datetime(2024, 6, 22),
    catchup=False
)

def step_1(**context):
    if not os.path.exists(filePath):
        raise FileNotFoundError(f"Cannot found at: {filePath}")
    
    size = os.path.getsize(filePath)

    if size > bytesThreshold:
        return "step_3"
    
    return "step_2"

def step_2(**context):
    df = pd.read_csv(filePath)
    print(f"File light summary\n{df.info()}")

def step_3(**context):
    df = pd.read_csv(filePath)
    print(f"Detailed processing\n{df.describe()}")

def step_4(**context):
    df = pd.read_csv(filePath)
    df.dropna(inplace=True)

step_1_branch_task = BranchPythonOperator(
    dag=dag,
    task_id="branch_operator",
    python_callable=step_1
)

step_2_task = PythonOperator(
    dag=dag,
    python_callable=step_2,
    task_id="step_2"
)

step_3_task = PythonOperator(
    dag=dag,
    python_callable=step_3,
    task_id="step_3"
) 

step_4_task = PythonOperator(
    dag=dag,
    python_callable=step_4,
    task_id="step_4",
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
)

step_1_branch_task >> [step_2_task, step_3_task] >> step_4_task
