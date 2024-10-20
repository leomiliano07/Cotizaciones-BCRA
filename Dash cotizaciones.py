import psycopg2
from psycopg2 import sql
import pandas as pd
import os
from dotenv import load_dotenv
from dash import Dash, html, dcc
import plotly.express as px

try:
    load_dotenv()
except Exception as e:
    print(f"Error al cargar el archivo .env: {e}")
    exit(1)

variables_requeridas = [
    "REDSHIFT_USER",
    "REDSHIFT_PASSWORD",
    "REDSHIFT_HOST",
    "REDSHIFT_PORT",
    "REDSHIFT_DATABASE",
    "REDSHIFT_SCHEMA"
]

variables_faltantes = [var for var in variables_requeridas if os.getenv(var) is None]

if variables_faltantes:
    print(f"Error: Faltan las siguientes variables de entorno: {', '.join(variables_faltantes)}")
    print("Asegúrate de que tu archivo .env contiene las siguientes líneas:")
    print("\n".join([f"{var}=valor" for var in variables_faltantes]))
    exit(1)

username = os.getenv("REDSHIFT_USER")
password = os.getenv("REDSHIFT_PASSWORD")
host = os.getenv("REDSHIFT_HOST")
port = os.getenv("REDSHIFT_PORT")
database = os.getenv("REDSHIFT_DATABASE")
schema = os.getenv("REDSHIFT_SCHEMA")

conn_string = f"host={host} dbname={database} user={username} password={password} port={port}"

try:
    conn = psycopg2.connect(conn_string)
    print("Conexión exitosa a la base de datos Redshift")
        
    with conn.cursor() as cursor:
        cursor.execute(sql.SQL("SET search_path TO {};").format(sql.Identifier(schema)))
        print(f"El esquema se ha configurado a {schema}")

    # Consulta SQL para obtener los datos de la tabla cotizaciones
    query_cotizaciones = f'SELECT fecha, codigo_moneda, cotizacion FROM "{schema}".cotizaciones;'
    df_cotizaciones = pd.read_sql(query_cotizaciones, conn)
    print(f"DataFrame de cotizaciones generado con {len(df_cotizaciones)} filas y {len(df_cotizaciones.columns)} columnas")
    
    # Consulta SQL para obtener los datos de la tabla moneda
    query_moneda = f'SELECT codigo_moneda, nombre FROM "{schema}".moneda;'
    df_moneda = pd.read_sql(query_moneda, conn)
    print(f"DataFrame de moneda generado con {len(df_moneda)} filas y {len(df_moneda.columns)} columnas")
    
    # Hacer el join entre cotizaciones y moneda
    df_merged = pd.merge(df_cotizaciones, df_moneda, on='codigo_moneda', how='left')
    print(f"DataFrame combinado generado con {len(df_merged)} filas y {len(df_merged.columns)} columnas")
    print(df_merged.head())
    
    conn.close()
    print("Conexión cerrada correctamente")

except psycopg2.Error as e:
    print(f"Error al conectarse a la base de datos: {e}")
    exit(1)

# Crear la aplicación Dash
app = Dash(__name__)

# Crear el gráfico de evolución de cotizaciones, usando el nombre de la moneda en lugar del código
fig = px.line(df_merged, x='fecha', y='cotizacion', color='nombre', 
              title='Evolución Diaria de las Cotizaciones por Nombre de Moneda',
              labels={'cotizacion': 'Cotización', 'fecha': 'Fecha', 'nombre': 'Moneda'},
              category_orders={"nombre": sorted(df_merged['nombre'].unique())})   

# Definir el layout de la aplicación
app.layout = html.Div(children=[
    html.H1(children='Evolución Diaria de las Cotizaciones por Moneda'),
    dcc.Graph(
        id='cotizaciones-graph',
        figure=fig
    )
])

# Ejecutar la aplicación
if __name__ == '__main__':
    app.run_server(debug=True)


