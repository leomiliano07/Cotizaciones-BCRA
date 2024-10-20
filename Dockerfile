FROM apache/airflow:2.10.2

COPY requirements.txt /

# Instala todas las librerías menos psycopg2
RUN pip install --no-cache-dir "apache-airflow==2.10.2" -r /requirements.txt