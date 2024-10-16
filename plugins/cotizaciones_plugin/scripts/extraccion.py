import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import pandas as pd
import psycopg2
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta

# Desactivar advertencias de solicitudes inseguras
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# URL de la API
url = "https://api.bcra.gob.ar/estadisticascambiarias/v1.0/Cotizaciones"

def get_last_loaded_date(redshift_host, redshift_port, redshift_database, redshift_user, redshift_password, redshift_schema, redshift_tabla):
    conn = None
    last_date = None
    try:
        conn = psycopg2.connect(
            host=redshift_host,
            port=redshift_port,
            database=redshift_database,
            user=redshift_user,
            password=redshift_password
        )
        cur = conn.cursor()
        
        # Consultar la última fecha cargada
        query = f"""
            SELECT MAX(FECHA) FROM "{redshift_schema}"."{redshift_tabla}"
        """
        cur.execute(query)
        last_date = cur.fetchone()[0]  # Obtener el resultado
        cur.close()
    except Exception as e:
        print(f"Error al consultar la última fecha cargada en Redshift: {e}")
    finally:
        if conn:
            conn.close()
    return last_date

def extract_data():
    # Obtener credenciales de Redshift desde Airflow
    connection = BaseHook.get_connection("Redshift")
    redshift_host = connection.host
    redshift_port = connection.port
    redshift_database = connection.schema
    redshift_user = connection.login
    redshift_password = connection.password

    # Obtener la última fecha cargada en Redshift
    last_loaded_date = get_last_loaded_date(redshift_host, redshift_port, redshift_database, redshift_user, redshift_password, '2024_leonardo_ezequiel_miliano_schema', 'cotizaciones')

    # Definir la fecha de inicio para la extracción
    if last_loaded_date:
        start_date = last_loaded_date + timedelta(days=1)  # Desde el día siguiente a la última fecha cargada
    else:
        start_date = datetime(2024, 1, 1)  # Cambia esta fecha a la más temprana que desees

    # Obtener la fecha actual
    end_date = datetime.now()

    # Asegurarse de que start_date y end_date son del mismo tipo (datetime.date)
    start_date = start_date.date() if isinstance(start_date, datetime) else start_date
    end_date = end_date.date()  # Convierte end_date a datetime.date

    # Inicializar lista para almacenar datos acumulados
    accumulated_data = []

    # Realizar solicitudes para cada día faltante
    current_date = start_date
    while current_date <= end_date:
        formatted_date = current_date.strftime('%Y-%m-%d')
        response = requests.get(f"{url}?fecha={formatted_date}", verify=False)

        if response.status_code == 200:
            data = response.json()
            if 'results' in data:
                accumulated_data.append(data['results'])  # Acumular resultados
            else:
                print(f"No se encontraron resultados para la fecha {formatted_date}.")
        else:
            print(f"Error en la solicitud para la fecha {formatted_date}: {response.status_code} - {response.text}")

        current_date += timedelta(days=1)  # Avanzar al siguiente día

    return accumulated_data if accumulated_data else None  # Devolver todos los resultados acumulados

