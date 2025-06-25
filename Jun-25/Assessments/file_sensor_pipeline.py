import pandas as pd
import os, shutil
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from pathlib import Path

filePath = r"/opt/airflow/data/incoming/report.csv"
archievePath = r"/opt/airflow/data/archive"

args = {
    "retries": 2,
    "retry_delay": timedelta(seconds=45)
}

dag = DAG(
    dag_id="file_sensor_pipeline",
    default_args=args,
    start_date=datetime(2025, 6, 25),
    schedule="@daily",
    catchup=False
)

def process_file(**context):
    df = pd.read_csv(filePath)
    df.dropna(inplace=True)
    ti = context["ti"]
    ti.xcom_push(value=df.to_dict(), key="processed_file")

def move_file(**context):
    ti = context["ti"]
    dictdf = ti.xcom_pull(key="processed_file")
    df = pd.DataFrame.from_dict(dictdf)
    df.to_csv(archievePath)

filesensor_task = FileSensor(
    task_id="fetch_file",
    filepath=filePath,
    timeout=600,
    poke_interval=30,
    dag=dag
)

process_task = PythonOperator(
    task_id="process_file",
    python_callable=process_file,
    dag=dag
)

move_task = PythonOperator(
    task_id="move_file",
    python_callable=move_file,
    dag=dag
)

filesensor_task >> process_task >> move_task