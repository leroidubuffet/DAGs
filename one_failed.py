from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

default_args = {
    'start_date': datetime(2023, 1, 1),
}

dag = DAG(
    'one_failed',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
)

task13 = EmptyOperator(task_id='task13', dag=dag)
task14 = EmptyOperator(task_id='task14', dag=dag)
task15 = EmptyOperator(task_id='task15', trigger_rule='one_failed', dag=dag)

task13 >> task15
task14 >> task15
