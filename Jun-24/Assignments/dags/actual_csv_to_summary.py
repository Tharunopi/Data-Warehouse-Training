from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

import os
import pandas as pd

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 6, 21),
}

file = r"/opt/airflow/dags/customer.csv"  

def step_1():
    if os.path.exists(file):
        print("File found!")
    else:
        raise FileNotFoundError(f"{file} not found!")

def step_2_step_3():
    df = pd.read_csv(file)
    rowLen = df.shape[0]
    print(f"Total Rows: {rowLen}")
    return "bash_task" if rowLen > 100 else "no_task"

dag = DAG(
    dag_id="csv_to_summary",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) 

with dag:

    step_1_task = PythonOperator(
        task_id="csv_check",
        python_callable=step_1,
    )

    step_2_step_3_task = BranchPythonOperator(
        task_id="csv_process",
        python_callable=step_2_step_3,
    )

    step_4_task = BashOperator(
        task_id="bash_task",
        bash_command="echo 'Row count > 100!'",
    )

    no_task = EmptyOperator(task_id="no_task")

    step_1_task >> step_2_step_3_task >> [step_4_task, no_task]
