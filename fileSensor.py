from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import os

BASE_DIR = '/path/in/container/'

def check_and_extract():
    try:
        # Obtener el primer archivo del directorio
        files = os.listdir(f'{BASE_DIR}altas/')
        if not files:
            raise Exception("No hay archivos.")
        file_path = f'{BASE_DIR}altas/{files[0]}'
        with open(file_path, 'r') as file:
            data = file.read().strip().split()
        if len(data) != 2:
            raise Exception("El archivo no contiene exactamente dos palabras.")
        os.remove(file_path)
        return data
    except Exception as e:
        raise Exception(f"Error procesando el archivo: {str(e)}")

def create_folder(directory, name):
    os.makedirs(f'{BASE_DIR}{directory}/{name}', exist_ok=True)

def add_to_csv(first_name, last_name):
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(f'{BASE_DIR}clientes/lista_altas.csv', 'a') as file:
        file.write(f'{first_name},{last_name},{current_time}\n')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 5, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('FileSensor', default_args=default_args, schedule_interval=timedelta(minutes=1)) as dag:
    wait_for_file = FileSensor(
        task_id='wait_for_file',
        filepath=f'{BASE_DIR}altas/*',
        poke_interval=60,
        timeout=600
    )

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=check_and_extract,
        do_xcom_push=True
    )

    create_name_folder = PythonOperator(
        task_id='create_name_folder',
        python_callable=create_folder,
        op_args=['nombres', '{{ ti.xcom_pull(task_ids="extract_task")[0] }}']
    )

    create_surname_folder = PythonOperator(
        task_id='create_surname_folder',
        python_callable=create_folder,
        op_args=['apellidos', '{{ ti.xcom_pull(task_ids="extract_task")[1] }}']
    )

    update_csv = PythonOperator(
        task_id='update_csv',
        python_callable=add_to_csv,
        op_args=[
            '{{ ti.xcom_pull(task_ids="extract_task")[0] }}',
            '{{ ti.xcom_pull(task_ids="extract_task")[1] }}'
        ]
    )

    wait_for_file >> extract_task >> [create_name_folder, create_surname_folder, update_csv]
