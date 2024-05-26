from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

default_args = {
    'start_date': datetime(2023, 1, 1),
}

dag = DAG(
    'all_done',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
)

task7 = EmptyOperator(task_id='task7', dag=dag)
task8 = EmptyOperator(task_id='task8', dag=dag)
task9 = EmptyOperator(task_id='task9', trigger_rule='all_done', dag=dag)

task7 >> task9
task8 >> task9
