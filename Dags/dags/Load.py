import pandas as pd
import logging
from datetime import datetime
from sqlalchemy.orm import sessionmaker, aliased
import sys
import os

# Añadir la ruta para las conexiones a la base de datos u otros módulos
sys.path.append("/home/ana/workshop2/src")
import db_connection  # Asegúrate de que este módulo existe y está correctamente configurado

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Load")

def load_to_postgres(**kwargs) -> None:
    """
    Carga el dataset fusionado a una tabla en la base de datos PostgreSQL.
    """
    try:
        print(f"[{datetime.now()}] - Iniciando carga de datos a PostgreSQL.")
        
        ti = kwargs['ti']
        
        # Recuperar los datos fusionados desde XCom
        merged_json = ti.xcom_pull(task_ids='merge_dataset')
        if not merged_json:
            raise ValueError("No se recibieron datos fusionados para cargar en PostgreSQL.")
        
        # Convertir el JSON a DataFrame
        df_merge = pd.read_json(merged_json, orient='records')
        
        print(f"[{datetime.now()}] - Datos fusionados convertidos a DataFrame.")
        
        # Obtener la conexión a PostgreSQL
        
        # Cargar el DataFrame a la tabla 'merge' en PostgreSQL
        engine = db_connection.conn() 
        Session = sessionmaker(bind=engine)
        session = Session
        df_merge.to_sql("merge", con=engine, if_exists="replace", index_label="id")
        
        print(f"[{datetime.now()}] - Datos cargados exitosamente en la tabla 'merge' de PostgreSQL.")
        
    except Exception as err:
        print(f"[{datetime.now()}] - Error en la carga a PostgreSQL: {err}")
        raise  # Re-lanza la excepción para que Airflow pueda manejarla