import requests
import pandas as pd
from requests.packages.urllib3.exceptions import InsecureRequestWarning # type: ignore
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import time

# Desactivar las advertencias de solicitudes inseguras
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

load_dotenv()

redshift_host = os.getenv("REDSHIFT_HOST")
redshift_port = os.getenv("REDSHIFT_PORT")
redshift_database = os.getenv("REDSHIFT_DATABASE")
redshift_user = os.getenv("REDSHIFT_USER")
redshift_password = os.getenv("REDSHIFT_PASSWORD")

url = "https://api.bcra.gob.ar/estadisticascambiarias/v1.0/Cotizaciones"

# Fechas de inicio y fin
start_date = "2024-01-01"
end_date = datetime.now().strftime("%Y-%m-%d")

monthly_data = pd.DataFrame()

# Fechas a tipo datetime
current_date = pd.to_datetime(start_date)
end_date = pd.to_datetime(end_date)

# Obtener el mes actual
current_month = current_date.month

# Conexión a Redshift
def get_redshift_connection():
    return psycopg2.connect(
        host=redshift_host,
        port=redshift_port,
        database=redshift_database,
        user=redshift_user,
        password=redshift_password
    )

# Función para verificar si ya existen registros para una fecha en Redshift
def datos_ya_existen(fecha, conn):
    with conn.cursor() as cur:
        check_query = sql.SQL("""
            SELECT COUNT(*) FROM {}.{} WHERE fecha = %s;
        """).format(sql.Identifier('2024_leonardo_ezequiel_miliano_schema'), sql.Identifier('cotizaciones'))
        cur.execute(check_query, (fecha,))
        count = cur.fetchone()[0]
        return count > 0

while current_date <= end_date:
    formatted_date = current_date.strftime("%Y-%m-%d")
    
    try:
        response = requests.get(f"{url}?fecha={formatted_date}", verify=False)

        if response.status_code == 200:
            data = response.json()
            if 'results' in data and 'detalle' in data['results']:
                fecha = data['results'].get('fecha', None)
                cotizaciones = data['results']['detalle']

                if not cotizaciones:
                    print(f"No hay cotizaciones disponibles para la fecha: {formatted_date}")
                else:
                    df = pd.DataFrame(cotizaciones)
                    if 'codigoMoneda' in df.columns and 'tipoCotizacion' in df.columns:
                        df['FECHA'] = fecha
                        df = df[['FECHA', 'codigoMoneda', 'tipoCotizacion']]
                        df.columns = ['FECHA', 'CÓDIGO MONEDA', 'COTIZACIÓN']
                        df['FECHA'] = pd.to_datetime(df['FECHA'])
                        df = df[~df['CÓDIGO MONEDA'].isin(['ARS', 'XAG', 'XAU'])]
                        monthly_data = pd.concat([monthly_data, df], ignore_index=True)
                    else:
                        print(f"Columnas esperadas no encontradas para la fecha: {formatted_date}")
            else:
                print(f"No se encontraron resultados para la fecha: {formatted_date}")
        else:
            print(f"Error en la solicitud para la fecha {formatted_date}: {response.status_code}")

    except requests.exceptions.RequestException as e:
        print(f"Error al realizar la solicitud: {e}")

    # Verificar si se ha completado un mes y cargar los datos a Redshift
    if current_date.month != current_month or current_date == end_date:
        # Si hay datos del mes, subirlos a Redshift
        if not monthly_data.empty:
            try:
                
                conn = get_redshift_connection()

                # Verificar si ya existen datos para las fechas
                monthly_data['EXISTEN'] = monthly_data['FECHA'].apply(lambda x: datos_ya_existen(x.strftime('%Y-%m-%d'), conn))
                new_data = monthly_data[monthly_data['EXISTEN'] == False]  # Solo insertar las filas nuevas

                if not new_data.empty:
                    insert_query = sql.SQL("""
                        INSERT INTO {}.{} (FECHA, CODIGO_MONEDA, COTIZACION)
                        VALUES (%s, %s, %s)
                    """).format(sql.Identifier('2024_leonardo_ezequiel_miliano_schema'), sql.Identifier('cotizaciones'))

                    # Insertar los datos nuevos en Redshift
                    with conn.cursor() as cur:
                        for index, row in new_data.iterrows():
                            cur.execute(insert_query, (row['FECHA'].strftime('%Y-%m-%d'), row['CÓDIGO MONEDA'], row['COTIZACIÓN']))
                        conn.commit()

                    print(f"Datos del mes {current_month} insertados correctamente en Redshift.")

                else:
                    print(f"Todos los datos del mes {current_month} ya existen en Redshift.")

            except Exception as e:
                print(f"Error al conectar o insertar datos en Redshift: {e}")

            finally:
                if conn:
                    conn.close()

            # Vaciar el DataFrame para el siguiente mes
            monthly_data = pd.DataFrame()

        # Actualizar el mes actual
        current_month = current_date.month

    # Incrementar la fecha en un día
    current_date += timedelta(days=1)
    time.sleep(0.2)  # Espera 0.2 segundo entre solicitudes

