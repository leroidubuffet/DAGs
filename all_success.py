from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

default_args = {
    'start_date': datetime(2023, 1, 1),
}

dag = DAG(
    'all_success',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
)

task1 = EmptyOperator(task_id='task1', dag=dag)
task2 = EmptyOperator(task_id='task2', dag=dag)
task3 = EmptyOperator(task_id='task3', trigger_rule='all_success', dag=dag)

task1 >> task3
task2 >> task3
