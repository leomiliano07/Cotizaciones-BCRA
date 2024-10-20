import psycopg2
from psycopg2 import sql
import os
from dotenv import load_dotenv

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

# Crear la cadena de conexión
conn_string = f"host={host} dbname={database} user={username} password={password} port={port}"

# SQL para crear la tabla
create_table_query = sql.SQL("""
CREATE TABLE IF NOT EXISTS {}.cotizaciones (
    id INT IDENTITY(1,1),
    fecha DATE NOT NULL,
    codigo_moneda VARCHAR(10) NOT NULL,
    cotizacion NUMERIC(10, 4) NOT NULL,
    PRIMARY KEY (id)
)
""").format(sql.Identifier(schema))

# Conectar a Redshift y crear la tabla
try:
    with psycopg2.connect(conn_string) as conn:
        with conn.cursor() as cur:
            cur.execute(create_table_query)
            conn.commit()
    print(f"La tabla 'cotizaciones' ha sido creada exitosamente en el esquema '{schema}'.")
except psycopg2.Error as e:
    print(f"Error de Redshift al crear la tabla: {e}")
except Exception as e:
    print(f"Error inesperado al crear la tabla: {e}")