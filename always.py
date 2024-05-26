from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

default_args = {
    'start_date': datetime(2023, 1, 1),
}

dag = DAG(
    'always',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
)

task22 = EmptyOperator
