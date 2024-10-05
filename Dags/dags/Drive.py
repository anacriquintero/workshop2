import logging
from datetime import datetime
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from pydrive2.files import FileNotUploadedError
import os
import sys

# Configuración de logging

# Ruta a las credenciales de PyDrive
DIRECTORIO_CREDENCIALES = '/home/ana/workshop2/pyDrive/credentials_module.json'

def login():
    """
    Autentica y devuelve una instancia de GoogleDrive.
    """
    try:
        print(f"[{datetime.now()}] - Iniciando sesión en Google Drive.")
        gauth = GoogleAuth()
        gauth.DEFAULT_SETTINGS['client_config_file'] = DIRECTORIO_CREDENCIALES
        gauth.LoadCredentialsFile(DIRECTORIO_CREDENCIALES)
        
        if gauth.credentials is None:
            print(f"[{datetime.now()}] - No se encontraron credenciales, autenticando localmente.")
            gauth.LocalWebserverAuth(port_numbers=[8092])
        elif gauth.access_token_expired:
            print(f"[{datetime.now()}] - Token expirado, refrescando credenciales.")
            gauth.Refresh()
        else:
            print(f"[{datetime.now()}] - Autorización existente encontrada.")
            gauth.Authorize()
        
        gauth.SaveCredentialsFile(DIRECTORIO_CREDENCIALES)
        drive = GoogleDrive(gauth)
        print(f"[{datetime.now()}] - Sesión en Google Drive iniciada exitosamente.")
        return drive
    except Exception as e:
        print(f"[{datetime.now()}] - Error durante la autenticación en Google Drive: {e}")
        raise

def Store_data(**kwargs):
    """
    Sube el archivo 'merge.csv' a una carpeta específica en Google Drive.
    """
    try:
        print(f"[{datetime.now()}] - Iniciando tarea de subida a Google Drive.")
        
        # Recuperar la ruta del archivo desde las variables de Airflow o definirla aquí
        merge_csv_path = "/home/ana/workshop2/Data/merge.csv"
        if not os.path.exists(merge_csv_path):
            raise FileNotFoundError(f"El archivo {merge_csv_path} no existe.")
        
        # ID de la carpeta en Google Drive donde se subirá el archivo
        # Reemplaza 'your_folder_id_here' con el ID real de tu carpeta en Google Drive
        ID_FOLDER = '1cpXnNGnUchrfkAG0DgXjaG_YawoC53YQ'
        
        # Nombre del archivo en Google Drive
        nombre_archivo_drive = '/home/ana/workshop2/Data/merge.csv'
        
        # Autenticar y obtener una instancia de GoogleDrive
        drive = login()
        
        # Crear y subir el archivo a la carpeta especificada
        archivo = drive.CreateFile({
            'title': nombre_archivo_drive,
            'parents': [{'id': ID_FOLDER}]
        })
        archivo.SetContentFile(merge_csv_path)
        archivo.Upload()
        
        print(f"[{datetime.now()}] - Archivo '{nombre_archivo_drive}' subido exitosamente a la carpeta con ID '{ID_FOLDER}'.")
        
    except FileNotFoundError as fnf_error:
        print(f"[{datetime.now()}] - {fnf_error}")
        raise
    except FileNotUploadedError as fue_error:
        print(f"[{datetime.now()}] - Error al subir el archivo a Google Drive: {fue_error}")
        raise
    except Exception as e:
        print(f"[{datetime.now()}] - Error durante la subida a Google Drive: {e}")
        raise
