from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

def print_hello_world():
    print("Hello, World!")

dag = DAG(
    'my_dag',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Sample DAG for start/end trigger',
    schedule_interval=None,
    start_date=datetime(2024, 10, 31),
    catchup=False,
)

start_task = PythonOperator(
    task_id='start_task',
    python_callable=lambda: print("DAG Started"),
    dag=dag,
)

end_task = PythonOperator(
    task_id='end_task',
    python_callable=print_hello_world,
    dag=dag,
)

start_task >> end_task
