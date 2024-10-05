import pandas as pd
import json
import logging
from datetime import datetime

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Merge")

def merge_data(**kwargs) -> None:
    """
    Fusiona los datasets de Spotify y Grammys utilizando XComs para obtener los datos procesados.
    Guarda el resultado en un archivo CSV.
    """
    try:
        print(f"[{datetime.now()}] - Iniciando la tarea de merge.")
        
        ti = kwargs['ti']
        
        # Recuperar los datos procesados de Spotify y Grammys desde XComs
        spotify_json = ti.xcom_pull(task_ids='drop_nulls_spotify_task')
        grammys_json = ti.xcom_pull(task_ids='drop_unnecessary_columns_task')
        
        if not spotify_json:
            raise ValueError("No se recibieron datos de Spotify para el merge.")
        if not grammys_json:
            raise ValueError("No se recibieron datos de Grammys para el merge.")
        
        # Convertir los JSON a DataFrames
        df_spotify = pd.read_json(spotify_json, orient='records')
        df_grammys = pd.read_json(grammys_json, orient='records')
        
        print(f"[{datetime.now()}] - Datos de Spotify y Grammys convertidos a DataFrames.")
        
        # Realizar el merge entre los datasets
        df_merge = pd.merge(df_grammys, df_spotify, left_on="nominee", right_on="track_name", how="inner")
        
        print(f"[{datetime.now()}] - Merge completado. Número de filas en el dataset fusionado: {len(df_merge)}")
        
        # Guardar el resultado en un archivo CSV
        merge_csv_path = "/home/ana/workshop2/Data/merge.csv"
        df_merge.to_csv(merge_csv_path, index=False)
        
        print(f"[{datetime.now()}] - Dataset fusionado guardado en {merge_csv_path}.")
        
    except Exception as err:
        print(f"[{datetime.now()}] - Error en la tarea de merge: {err}")
        raise