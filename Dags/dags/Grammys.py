import sys
import pandas as pd
import json
import logging
from datetime import datetime
from sqlalchemy.orm import sessionmaker, aliased

# Añadir la ruta para las conexiones a la base de datos u otros módulos
sys.path.append("/home/ana/workshop2/src/")

import db_connection  # Asegúrate de que este módulo existe y está correctamente configurado

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Grammys")

def ExtractGrammys() -> json:
    """
    Extrae datos de Grammys desde la base de datos PostgreSQL.
    """
    try:
        print(f"[{datetime.now()}] - Iniciando extracción de datos de Grammys.")
        engine = db_connection.conn() 
        Session = sessionmaker(bind=engine)
        session = Session
        query="SELECT * FROM grammys"
        df_grammys_raw = pd.read_sql(query, con=engine)
        print(f"[{datetime.now()}] - Datos de Grammys extraídos exitosamente. Número de filas: {len(df_grammys_raw)}")
        return df_grammys_raw.to_json(orient="records")
    except Exception as err:
        print(f"[{datetime.now()}] - Error al extraer datos de Grammys: {err}")
        return None

def normalize_columns(**kwargs) -> json:
    """
    Normaliza las columnas a mayúsculas, excepto 'winner' y columnas numéricas.
    """
    try:
        print(f"[{datetime.now()}] - Iniciando normalización de columnas.")
        ti = kwargs["ti"]
        json_data = ti.xcom_pull(task_ids="extract_grammys_task")
        if not json_data:
            raise ValueError("No se recibieron datos para normalizar columnas.")
        
        df = pd.read_json(json_data, orient="records")
        df_normalized = df.copy()
        
        # Seleccionar columnas de tipo objeto excluyendo 'winner'
        columns_to_normalize = df_normalized.select_dtypes(include='object').columns
        columns_to_normalize = [col for col in columns_to_normalize if col.lower() != 'winner']
        
        # Convertir a mayúsculas
        df_normalized[columns_to_normalize] = df_normalized[columns_to_normalize].apply(lambda x: x.str.upper())
        print(f"[{datetime.now()}] - Normalización de columnas completada.")
        return df_normalized.to_json(orient="records")
    except Exception as err:
        print(f"[{datetime.now()}] - Error en normalización de columnas: {err}")
        return None

def eliminate_null_nominee(**kwargs) -> json:
    """
    Elimina filas donde la columna 'nominee' es nula.
    """
    try:
        print(f"[{datetime.now()}] - Iniciando eliminación de filas con 'nominee' nulo.")
        ti = kwargs["ti"]
        json_data = ti.xcom_pull(task_ids="normalize_columns_task")
        if not json_data:
            raise ValueError("No se recibieron datos para eliminar valores nulos en 'nominee'.")
        
        df = pd.read_json(json_data, orient="records")
        dataframe_cleaned = df.dropna(subset=['nominee'])
        print(f"[{datetime.now()}] - Eliminación de filas con 'nominee' nulo completada. Filas restantes: {len(dataframe_cleaned)}")
        return dataframe_cleaned.to_json(orient="records")
    except Exception as err:
        print(f"[{datetime.now()}] - Error al eliminar valores nulos en 'nominee': {err}")
        return None

def filter_categories(**kwargs) -> json:
    """
    Filtra las filas donde la categoría comienza con "REMIXER" o "BEST NEW COUNTRY &".
    """
    try:
        print(f"[{datetime.now()}] - Iniciando filtrado de categorías específicas.")
        ti = kwargs["ti"]
        json_data = ti.xcom_pull(task_ids="eliminate_null_nominee_task")
        if not json_data:
            raise ValueError("No se recibieron datos para filtrar categorías.")
        
        df = pd.read_json(json_data, orient="records")
        df_filtered = df[df['category'].str.startswith("REMIXER") | df['category'].str.startswith("BEST NEW COUNTRY &")]
        print(f"[{datetime.now()}] - Filtrado de categorías completado. Filas restantes: {len(df_filtered)}")
        return df_filtered.to_json(orient="records")
    except Exception as err:
        print(f"[{datetime.now()}] - Error al filtrar categorías: {err}")
        return None

def drop_specific_rows(**kwargs) -> json:
    """
    Elimina filas específicas por índice.
    """
    try:
        print(f"[{datetime.now()}] - Iniciando eliminación de filas específicas.")
        ti = kwargs["ti"]
        json_data = ti.xcom_pull(task_ids="filter_categories_task")
        if not json_data:
            raise ValueError("No se recibieron datos para eliminar filas específicas.")
        
        df = pd.read_json(json_data, orient="records")
        indices_to_drop = df.dropna(inplace=True)
        df_dropped = df.drop(index=indices_to_drop, errors='ignore')
        print(f"[{datetime.now()}] - Eliminación de filas específicas completada. Filas restantes: {len(df_dropped)}")
        return df_dropped.to_json(orient="records")
    except Exception as err:
        print(f"[{datetime.now()}] - Error al eliminar filas específicas: {err}")
        return None

def drop_unnecessary_columns(**kwargs) -> json:
    """
    Elimina columnas innecesarias: ['img', 'title', 'published_at', 'updated_at', 'workers'].
    """
    try:
        print(f"[{datetime.now()}] - Iniciando eliminación de columnas innecesarias.")
        ti = kwargs["ti"]
        json_data = ti.xcom_pull(task_ids="drop_specific_rows_task")
        if not json_data:
            raise ValueError("No se recibieron datos para eliminar columnas innecesarias.")
        
        df = pd.read_json(json_data, orient="records")
        columns_to_drop = ['img', 'title', 'published_at', 'updated_at', 'workers']
        df_final = df.drop(columns=columns_to_drop, errors='ignore')
        print(f"[{datetime.now()}] - Eliminación de columnas innecesarias completada. Columnas restantes: {list(df_final.columns)}")
        return df_final.to_json(orient="records")
    except Exception as err:
        print(f"[{datetime.now()}] - Error al eliminar columnas innecesarias: {err}")
        return None