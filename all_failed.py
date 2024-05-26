from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

default_args = {
    'start_date': datetime(2023, 1, 1),
}

dag = DAG(
    'all_failed',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
)

task4 = EmptyOperator(task_id='task4', dag=dag)
task5 = EmptyOperator(task_id='task5', dag=dag)
task6 = EmptyOperator(task_id='task6', trigger_rule='all_failed', dag=dag)

task4 >> task6
task5 >> task6
