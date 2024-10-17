FROM apache/airflow:2.10.2

COPY requirements.txt /

# Instala todas las librerías menos psycopg2
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r <(grep -v 'psycopg2' /requirements.txt)