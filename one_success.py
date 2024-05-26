from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

default_args = {
    'start_date': datetime(2023, 1, 1),
}

dag = DAG(
    'one_success',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
)

task10 = EmptyOperator(task_id='task10', dag=dag)
task11 = EmptyOperator(task_id='task11', dag=dag)
task12 = EmptyOperator(task_id='task12', trigger_rule='one_success', dag=dag)

task10 >> task12
task11 >> task12
