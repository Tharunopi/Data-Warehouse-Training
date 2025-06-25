import random
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable

email_recip = Variable.get("email_recipt", default_var="tharunaadhi8@gmail.com")

args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": True,
    "email": [email_recip]
}

dag = DAG(
    dag_id="email_notification",
    start_date=datetime(2025, 6, 25),
    schedule="@daily",
    catchup=False,
    default_args=args
)

def func_1(**context):
    value = random.random()
    if value > 0.50:
        raise Exception("func_1 failure")
    
def func_2(**context):
    value = random.random()
    if value > 0.50:
        raise Exception("func_2 failure")
    
task_1 = PythonOperator(
    task_id="task_1",
    python_callable=func_1,
    dag=dag
)

task_2 = PythonOperator(
    task_id="task_2",
    python_callable=func_2,
    dag=dag
)

email_task = EmailOperator(
    task_id="success_mail",
    to=email_recip,
    subject="Airflow DAG success: {{ dag.dag_id }}",
    html_content="All tasks in DAG {{ dag.dag_id }} completed successfully",
    trigger_rule="all_success",
    dag=dag
)

task_1 >> task_2 >> email_task