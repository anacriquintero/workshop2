from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Asegúrate de que la ruta a tus funciones sea correcta
# Supongamos que tus archivos de funciones están en la carpeta 'dags/functions'
sys.path.append(os.path.join(os.path.dirname(__file__), 'functions'))

from Spotify import ExtractSpotify, filter_numeric_columns, drop_nulls
from Grammys import (
    ExtractGrammys,
    normalize_columns,
    eliminate_null_nominee,
    filter_categories,
    drop_specific_rows,
    drop_unnecessary_columns
)
from Merge import merge_data
from Load import load_to_postgres
from Drive import Store_data  # Importar la función de Store

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'Spotify_Grammys_ETL',
    default_args=default_args,
    description='ETL process for Spotify and Grammys data',
    schedule_interval=timedelta(days=1),
)

def log_task_execution(task_name, **kwargs):
    print(f"Executing task: {task_name}")

with dag:
    # Tareas de Spotify
    extract_spotify_task = PythonOperator(
        task_id='extract_spotify_task',
        python_callable=ExtractSpotify,
    )

    filter_numeric_columns_task = PythonOperator(
        task_id='filter_numeric_columns_task',
        python_callable=filter_numeric_columns,
        provide_context=True,
    )

    drop_nulls_spotify_task = PythonOperator(
        task_id='drop_nulls_spotify_task',
        python_callable=drop_nulls,
        provide_context=True,
    )

    # Tareas de Grammys
    extract_grammys_task = PythonOperator(
        task_id='extract_grammys_task',
        python_callable=ExtractGrammys,
    )

    normalize_columns_task = PythonOperator(
        task_id='normalize_columns_task',
        python_callable=normalize_columns,
        provide_context=True,
    )

    eliminate_null_nominee_task = PythonOperator(
        task_id='eliminate_null_nominee_task',
        python_callable=eliminate_null_nominee,
        provide_context=True,
    )

    filter_categories_task = PythonOperator(
        task_id='filter_categories_task',
        python_callable=filter_categories,
        provide_context=True,
    )

    drop_specific_rows_task = PythonOperator(
        task_id='drop_specific_rows_task',
        python_callable=drop_specific_rows,
        provide_context=True,
    )

    drop_unnecessary_columns_task = PythonOperator(
        task_id='drop_unnecessary_columns_task',
        python_callable=drop_unnecessary_columns,
        provide_context=True,
    )

    # Tarea de Merge
    merge_dataset = PythonOperator(
        task_id='merge_dataset',
        python_callable=merge_data,
        provide_context=True,
    )

    # Tarea de Carga
    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_to_postgres,
        provide_context=True,
    )

    # Tarea de Almacenamiento en Drive
    drive_upload = PythonOperator(
        task_id="Drive_upload",
        python_callable=Store_data,
        provide_context=True,
    )

    # Definición de dependencias
    # Tareas de Spotify
    extract_spotify_task >> filter_numeric_columns_task >> drop_nulls_spotify_task >> merge_dataset

    # Tareas de Grammys
    extract_grammys_task >> normalize_columns_task >> eliminate_null_nominee_task >> filter_categories_task >> drop_specific_rows_task >> drop_unnecessary_columns_task >> merge_dataset

    # Merge y Carga
    merge_dataset >> load_data >> drive_upload
