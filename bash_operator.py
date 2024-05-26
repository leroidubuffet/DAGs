from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

BASE_DIR = '/path/in/container/'

import os
from airflow.exceptions import AirflowException

def check_and_extract():
    try:
        # Lista los archivos en el directorio 'altas/'
        files = os.listdir(f'{BASE_DIR}altas/')
    except PermissionError:
        raise AirflowException(f"Error de permisos al intentar listar el directorio {BASE_DIR}altas/")

    if files:
        file_path = f'{BASE_DIR}altas/{files[0]}'
        try:
            with open(file_path, 'r') as file:
                # Suponemos que el archivo contiene una sola línea con dos palabras
                data = file.read().strip().split()
            # Verificar que hay exactamente dos palabras
            if len(data) != 2:
                raise AirflowException("El archivo no contiene exactamente dos palabras.")
        except FileNotFoundError:
            raise AirflowException(f"El archivo {file_path} no fue encontrado.")
        except PermissionError:
            raise AirflowException(f"Error de permisos al intentar leer el archivo {file_path}.")
        except Exception as e:
            raise AirflowException(f"Error al leer el archivo {file_path}: {str(e)}")
        
        try:
            # Elimina el archivo después de leerlo
            os.remove(file_path)
        except PermissionError:
            raise AirflowException(f"Error de permisos al intentar eliminar el archivo {file_path}.")
        except Exception as e:
            raise AirflowException(f"Error al eliminar el archivo {file_path}: {str(e)}")

        return data
    else:
        raise AirflowException(f"No se encontraron archivos en {BASE_DIR}altas/")


def create_folder(directory, name):
    # Crea un directorio usando la primera palabra como nombre del directorio
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

# Establecer `schedule_interval=None` para que el DAG solo se ejecute cuando se active manualmente
with DAG('bash_signup_dag_v2', default_args=default_args, schedule_interval=timedelta(minutes=1)) as dag:
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

    # Configura las dependencias
    extract_task >> [create_name_folder, create_surname_folder, update_csv]
