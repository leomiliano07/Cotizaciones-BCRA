FROM apache/airflow:2.10.2

COPY requirements.txt /

# Instala todas las librerías menos psycopg2 y dash
RUN grep -Ev '^(psycopg2|dash)$' /requirements.txt > /filtered_requirements.txt && \
    pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /filtered_requirements.txt && \
    rm /filtered_requirements.txt
