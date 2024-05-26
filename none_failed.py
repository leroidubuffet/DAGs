from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

default_args = {
    'start_date': datetime(2023, 1, 1),
}

dag = DAG(
    'none_failed',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
)

task16 = EmptyOperator(task_id='task16', dag=dag)
task17 = EmptyOperator(task_id='task17', dag=dag)
task18 = EmptyOperator(task_id='task18', trigger_rule='none_failed', dag=dag)

task16 >> task18
task17 >> task18
