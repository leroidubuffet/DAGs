from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
}

dag = DAG(
    'depends_on_past',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['example'],
)

task1 = EmptyOperator(
    task_id='task1',
    dag=dag
)

task2 = EmptyOperator(
    task_id='task2',
    depends_on_past=True,
    dag=dag
)

task1 >> task2
