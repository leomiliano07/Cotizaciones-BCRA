import requests
import pandas as pd
from requests.packages.urllib3.exceptions import InsecureRequestWarning

# Desactivar las advertencias de solicitudes inseguras
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# URL de la API
url = "https://api.bcra.gob.ar/estadisticascambiarias/v1.0/Cotizaciones"

try:
    # Realizar la solicitud GET sin verificar el certificado SSL
    response = requests.get(url, verify=False)

    # Verificar si la solicitud fue exitosa
    if response.status_code == 200:
        # La solicitud fue exitosa
        data = response.json()

        # Verificar si 'results' y 'detalle' están en la respuesta
        if 'results' in data and 'detalle' in data['results']:
            # Extraer la fecha y los detalles de las cotizaciones
            fecha = data['results'].get('fecha', None)
            cotizaciones = data['results']['detalle']

            # Crear un DataFrame de pandas
            df = pd.DataFrame(cotizaciones)

            # Añadir la columna de fecha
            df['FECHA'] = fecha

            # Reordenar las columnas para que la fecha esté primero y poner los títulos en mayúsculas
            df = df[['FECHA', 'codigoMoneda', 'tipoCotizacion']]
            df.columns = ['FECHA', 'CÓDIGO MONEDA', 'COTIZACIÓN']

            df['FECHA'] = pd.to_datetime(df['FECHA'])

            # Filtrar para eliminar las filas donde 'CÓDIGO MONEDA' sea 'ARS'
            df = df[df['CÓDIGO MONEDA'] != 'ARS']
        else:
            print("Error: No se encontraron los resultados esperados en la respuesta.")
            df = pd.DataFrame()

    else:
        # La solicitud falló
        print(f"Error en la solicitud: {response.status_code}")
        print(response.text)
        df = pd.DataFrame()

except requests.exceptions.RequestException as e:
    print(f"Error al realizar la solicitud: {e}")
    df = pd.DataFrame()

print(df)