import logging
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator

filePath = r"/opt/airflow/data/orders.csv"
columns = {"productID", "productName", "quantity", "price"}

dag = DAG(
    dag_id="data_quality_validation",
    schedule="@daily",
    start_date=datetime(2025,6,25),
    catchup=False
)

def readData(**context):
    df = pd.read_csv(filePath)
    ti = context["ti"]
    ti.xcom_push(key="readData", value=df.to_dict())

def validateColumns(**context):
    ti = context["ti"]
    df = ti.xcom_pull(key="readData")
    df = pd.DataFrame.from_dict(df)
    if columns.issubset(set(df.columns)):
        return "summary"
    return "stop_dag"

def summarization(**context):
    ti = context["ti"]
    df = ti.xcom_pull(key="readData")
    df = pd.DataFrame.from_dict(df)
    df.fillna(value=0, inplace=True)
    print(df.describe())

def stop_dag(**context):
    logging.error("Dag stopped")

read_task = PythonOperator(
    task_id="read_data",
    python_callable=readData,
    dag=dag
)

validate_task = BranchPythonOperator(
    task_id="validate",
    python_callable=validateColumns,
    dag=dag
)

summary_task = PythonOperator(
    task_id="summary",
    python_callable=summarization,
    dag=dag
)

stop_task = PythonOperator(
    task_id="stop_dag",
    python_callable=stop_dag,
    dag=dag
)

read_task >> validate_task  
validate_task >> summary_task
validate_task >> stop_task