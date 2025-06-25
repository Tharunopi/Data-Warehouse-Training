import json, logging
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

dag = DAG(
    dag_id="external_api_",
    start_date=datetime(2025, 6, 24),
    schedule="@daily",
    catchup=False
)

get_btc_price = SimpleHttpOperator(
    task_id="get_btc_price",
    http_conn_id="coin_api",  
    endpoint="v1/exchangerate/BTC/USD",
    method="GET",
    headers={"X-CoinAPI-Key": "d5e85db4-4d83-4710-8bcf-c9d96945e49b"}, 
    response_filter=lambda response: json.loads(response.text),
    log_response=True,
    dag=dag
)

def log_btc_price(**context):
    response_data = context["ti"].xcom_pull(task_ids="get_btc_price")
    if not response_data or "rate" not in response_data:
        raise ValueError("Invalid response: 'rate' not found")
    btc_price = response_data["rate"]
    logging.info(f"BTC to USD price: ${btc_price:.2f}")

log_price_task = PythonOperator(
    task_id="log_btc_price",
    python_callable=log_btc_price,
    dag=dag
)

get_btc_price >> log_price_task