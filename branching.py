from airflow.decorators import dag, task
from datetime import datetime
from airflow.operators.empty import EmptyOperator

# Define your DAG using the @dag decorator
@dag(schedule="@daily", start_date=datetime(2021, 1, 1), catchup=False)
def branching():
    @task
    def start_task():
        # Simula algún proceso que retorna un valor
        return "some_value"

    @task.branch
    def branch_func(value):
        if value == "option1":
            return "task_option1"
        else:
            return "task_option2"

    @task
    def task_option1():
        print("Ejecutando Opción 1")

    @task
    def task_option2():
        print("Ejecutando Opción 2")

    start_value = start_task()
    branch = branch_func(start_value)
    task1 = task_option1()
    task2 = task_option2()

    branch >> [task1, task2]

# Asigna el DAG al objeto branching_dag para que Airflow lo detecte
branching_dag = branching()
