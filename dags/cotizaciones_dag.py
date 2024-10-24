from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.hooks.base_hook import BaseHook
from cotizaciones_plugin.scripts.extraccion import extract_data
from cotizaciones_plugin.scripts.transformacion import transform_data
from cotizaciones_plugin.scripts.carga import load_to_redshift

redshift_schema = '2024_leonardo_ezequiel_miliano_schema'
redshift_tabla = 'cotizaciones'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 10),
    'retries': 1,
}

def get_redshift_connection():
    connection = BaseHook.get_connection("Redshift")
    return {
        'host': connection.host,
        'port': connection.port,
        'database': connection.schema,
        'user': connection.login,
        'password': connection.password
    }

with DAG(dag_id='dag_cotizaciones_bcra',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=True) as dag:

    # Extrae
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )

    # Transforma
    def transform_task_callable(**kwargs):
        data = kwargs['ti'].xcom_pull(task_ids='extract_data')
        return transform_data(data)
    
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_task_callable,
    )

    # Carga en Redshift
    def load_task_callable(**kwargs):
        df = kwargs['ti'].xcom_pull(task_ids='transform_data')
        redshift_conn = get_redshift_connection()
        
        load_to_redshift(df, 
                         redshift_conn['host'], 
                         redshift_conn['port'], 
                         redshift_conn['database'], 
                         redshift_conn['user'], 
                         redshift_conn['password'], 
                         redshift_schema, 
                         redshift_tabla)

    load_task = PythonOperator(
        task_id='load_to_redshift',
        python_callable=load_task_callable,
    )

    extract_task >> transform_task >> load_task
