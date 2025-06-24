# Automate a daily sales summarization process.

import shutil, os
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import TaskInstance

dag = DAG(
    dag_id="daily_sales",
    start_date=datetime(2025, 6, 24, 6),  
    schedule="@daily",
    dagrun_timeout=timedelta(minutes=5),
    catchup=False
)

readPath = "/opt/airflow/dags/sales_data/sales.csv"
currentPath = "/opt/airflow/dags/sales_data/sales.csv"
sourcePath = "/opt/airflow/dags/sales_data/summary"

def step_1_2_3(**context):
    df = pd.read_csv(readPath)

    dfGrouped = df.groupby(by="Category").agg(
        totalSales = pd.NamedAgg(column="Amount", aggfunc="sum")
    )

    context["ti"].xcom_push(key="dfGrouped", value=dfGrouped.to_dict())
    print(f"Summary saved.")

def step_4(**context):
    dfGrouped = pd.DataFrame.from_dict(context["ti"].xcom_pull(key="dfGrouped", task_ids="1_2_3"))
    os.makedirs(sourcePath, exist_ok=True)

    path = shutil.move(src=currentPath, dst=sourcePath)
    print(f"File moved to: {path}")

step_1_2_3_task = PythonOperator(
    dag=dag, 
    task_id="1_2_3",
    python_callable=step_1_2_3
    )

step_4_task = PythonOperator(
    dag=dag, 
    task_id="4",
    python_callable=step_4
    )

step_1_2_3_task >> step_4_task