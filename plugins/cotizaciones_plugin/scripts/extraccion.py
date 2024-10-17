import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import pandas as pd
import psycopg2
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

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
        
        # Consulta la última fecha cargada
        query = f"""
            SELECT MAX(FECHA) FROM "{redshift_schema}"."{redshift_tabla}"
        """
        cur.execute(query)
        last_date = cur.fetchone()[0]
        cur.close()
    except Exception as e:
        print(f"Error al consultar la última fecha cargada en Redshift: {e}")
    finally:
        if conn:
            conn.close()
    return last_date

def extract_data():
    
    connection = BaseHook.get_connection("Redshift")
    redshift_host = connection.host
    redshift_port = connection.port
    redshift_database = connection.schema
    redshift_user = connection.login
    redshift_password = connection.password

    last_loaded_date = get_last_loaded_date(redshift_host, redshift_port, redshift_database, redshift_user, redshift_password, '2024_leonardo_ezequiel_miliano_schema', 'cotizaciones')

    # Define la fecha de inicio para la extracción
    if last_loaded_date:
        start_date = last_loaded_date + timedelta(days=1)
    else:
        start_date = datetime(2024, 10, 10)

    # Obtiene la fecha actual
    end_date = datetime.now()

    # Asegura que todo sea formato fecha
    start_date = start_date.date() if isinstance(start_date, datetime) else start_date
    end_date = end_date.date()

    # Inicializa la lista para almacenar datos acumulados
    accumulated_data = []

    # Realiza solicitudes para cada día faltante
    current_date = start_date
    while current_date <= end_date:
        formatted_date = current_date.strftime('%Y-%m-%d')
        response = requests.get(f"{url}?fecha={formatted_date}", verify=False)

        if response.status_code == 200:
            data = response.json()
            if 'results' in data:
                accumulated_data.append(data['results'])
            else:
                print(f"No se encontraron resultados para la fecha {formatted_date}.")
        else:
            print(f"Error en la solicitud para la fecha {formatted_date}: {response.status_code} - {response.text}")

        current_date += timedelta(days=1)

    return accumulated_data if accumulated_data else None  # Devuelve todos los resultados acumulados

