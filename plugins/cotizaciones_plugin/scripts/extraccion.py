import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning # type: ignore
import psycopg2
import logging
try:
    from airflow.hooks.base_hook import BaseHook
except ImportError:
    class BaseHook:
        @staticmethod
        def get_connection(conn_id):
            # Retorna un objeto simulado si Airflow no está disponible para los test locales
            return Mock()
from datetime import datetime, timedelta

# Desactivar las advertencias de solicitudes inseguras
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# Configuración básica del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
        logging.info(f"Última fecha cargada recuperada: {last_date}")
    except Exception as e:
        logging.error(f"Error al consultar la última fecha cargada en Redshift: {e}")
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
        logging.info(f"Fecha de inicio para la extracción: {start_date}")
    else:
        start_date = datetime(2024, 10, 10)
        logging.info(f"No se encontró fecha previa, usando {start_date} como fecha de inicio.")

    # Obtiene la fecha actual
    end_date = datetime.now()

    # Todo en formato fecha
    start_date = start_date.date() if isinstance(start_date, datetime) else start_date
    end_date = end_date.date()

    logging.info(f"Fecha de fin para la extracción: {end_date}")

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
                logging.info(f"Datos extraídos para la fecha {formatted_date}.")
            else:
                logging.warning(f"No se encontraron resultados para la fecha {formatted_date}.")
        else:
            logging.error(f"Error en la solicitud para la fecha {formatted_date}: {response.status_code} - {response.text}")

        current_date += timedelta(days=1)

    if accumulated_data:
        logging.info("Extracción completada con éxito.")
    else:
        logging.warning("No se extrajeron datos.")

    return accumulated_data if accumulated_data else None  # Devuelve todos los resultados acumulados
