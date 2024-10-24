import psycopg2
from psycopg2 import sql
import logging

def load_to_redshift(df, redshift_host, redshift_port, redshift_database, redshift_user, redshift_password, redshift_schema, redshift_tabla):
    conn = None
    if df is not None:
        if df.empty:
            logging.info("No hay datos para insertar.")
            return

        conn = psycopg2.connect(
            host=redshift_host,
            port=redshift_port,
            database=redshift_database,
            user=redshift_user,
            password=redshift_password
        )
        cur = conn.cursor()

        insert_query = sql.SQL("""
            INSERT INTO {}.{} (FECHA, CODIGO_MONEDA, COTIZACION)
            VALUES (%s, %s, %s)
        """).format(sql.Identifier(redshift_schema), sql.Identifier(redshift_tabla))

        # Inserta los datos del DataFrame en la tabla
        for index, row in df.iterrows():
            cur.execute(insert_query, (row['FECHA'], row['CÓDIGO MONEDA'], row['COTIZACIÓN']))

        conn.commit()
        logging.info("Datos insertados correctamente en Redshift.")

        cur.close()
        conn.close()
    else:
        logging.info("No hay datos para insertar.")