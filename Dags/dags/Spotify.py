import pandas as pd
import json
import logging
from datetime import datetime

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Spotify")

def ExtractSpotify() -> json:
    """
    Extrae datos de Spotify leyendo un archivo CSV.
    """
    try:
        (f"[{datetime.now()}] - Iniciando extracción de datos de Spotify.")
        dataframe_raw = pd.read_csv("/home/ana/workshop2/Data/spotify_dataset.csv")
        print(f"[{datetime.now()}] - Datos de Spotify extraídos exitosamente. Número de filas: {len(dataframe_raw)}")
        return dataframe_raw.to_json(orient="records")
    except Exception as err:
        print(f"[{datetime.now()}] - Error al extraer datos de Spotify: {err}")
        return None

def filter_numeric_columns(**kwargs) -> json:
    """
    Filtra las columnas numéricas del DataFrame.
    """
    try:
        print(f"[{datetime.now()}] - Iniciando filtrado de columnas numéricas.")
        ti = kwargs["ti"]
        json_data = ti.xcom_pull(task_ids="extract_spotify_task")
        if not json_data:
            raise ValueError("No se recibieron datos para filtrar columnas numéricas.")
        
        df = pd.read_json(json_data, orient="records")
        numeric_columns = df.select_dtypes(include=['float64', 'int64']).columns
        df_numeric = df[numeric_columns]
        print(f"[{datetime.now()}] - Filtrado de columnas numéricas completado. Columnas retenidas: {list(numeric_columns)}")
        return df_numeric.to_json(orient="records")
    except Exception as err:
        print(f"[{datetime.now()}] - Error en filtrado de columnas numéricas: {err}")
        return None

def drop_nulls(**kwargs) -> json:
    """
    Elimina filas con valores nulos en columnas específicas.
    """
    try:
        print(f"[{datetime.now()}] - Iniciando eliminación de valores nulos.")
        ti = kwargs["ti"]
        json_data = ti.xcom_pull(task_ids="filter_numeric_columns_task")
        if not json_data:
            raise ValueError("No se recibieron datos para eliminar valores nulos.")
        
        df = pd.read_json(json_data, orient="records")
        columns_to_check = ['artists', 'album_name', 'track_name']
        dataframe_cleaned = df.dropna(subset=columns_to_check)
        print(f"[{datetime.now()}] - Eliminación de valores nulos completada. Filas restantes: {len(dataframe_cleaned)}")
        return dataframe_cleaned.to_json(orient="records")
    except Exception as err:
        print(f"[{datetime.now()}] - Error al eliminar valores nulos: {err}")
        return None