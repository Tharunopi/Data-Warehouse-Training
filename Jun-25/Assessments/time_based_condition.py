from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

dag = DAG(
    dag_id="time_based_condition",
    start_date=datetime(2025, 6, 25),
    schedule="@daily",
    catchup=False
)

def currentSystemTime(**context):
    time = int(datetime.now().strftime("%H"))
    weekDay = datetime.now().weekday()
    
    if weekDay >= 5:
        return "empty"

    if time >= 6 and time <=12:
        return "task_a"
    elif time > 12 and time <= 18:
        return "task_b"
    return "empty"
    
def morningGreet(**context):
    print("Good Morning!")

def afterNoon(**context):
    print("Good Afternoon")

def cleanup(**context):
    print("Cleaning up...")
    print("Done")

time_task = BranchPythonOperator(
    task_id="time_task",
    dag=dag,
    python_callable=currentSystemTime
)

mrng_task = PythonOperator(
    task_id="task_a",
    python_callable=morningGreet,
    dag=dag
)

eve_task = PythonOperator(
    task_id="task_b",
    python_callable=afterNoon,
    dag=dag
)

empty_task = EmptyOperator(
    task_id="empty",
    dag=dag
)

clean_task = PythonOperator(
    task_id="clean_up",
    python_callable=cleanup,
    dag=dag
)

time_task >> [mrng_task, eve_task, empty_task] >> clean_task