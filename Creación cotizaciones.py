import psycopg2
from psycopg2 import sql
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Define required environment variables
required_vars = ["REDSHIFT_USER", "REDSHIFT_PASSWORD", "REDSHIFT_HOST", "REDSHIFT_PORT", "REDSHIFT_DATABASE", "REDSHIFT_SCHEMA"]

# Check for missing environment variables
missing_vars = [var for var in required_vars if os.getenv(var) is None]

if missing_vars:
    print(f"Error: The following required environment variables are missing: {', '.join(missing_vars)}")
    print("Please check your .env file and ensure all required variables are set.")
    exit(1)

# Assign environment variables to variables
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
    print(f"La tabla 'cotizacionesss' ha sido creada exitosamente en el esquema '{schema}'.")
except psycopg2.Error as e:
    print(f"Error de Redshift al crear la tabla: {e}")
except Exception as e:
    print(f"Error inesperado al crear la tabla: {e}")