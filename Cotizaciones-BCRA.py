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

        # Extraer la fecha y los detalles de las cotizaciones
        fecha = data['results']['fecha']
        cotizaciones = data['results']['detalle']

        # Crear un DataFrame de pandas
        df = pd.DataFrame(cotizaciones)

        # Añadir la columna de fecha
        df['fecha'] = fecha

        # Reordenar las columnas para que la fecha esté primero
        df = df[['fecha', 'codigoMoneda', 'descripcion', 'tipoCotizacion']]
        # df = df[['fecha', 'codigoMoneda', 'descripcion', 'tipoPase', 'tipoCotizacion']]

        print("DataFrame creado exitosamente. Primeras 5 filas:")
        print(df.head())

        print("\nInformación del DataFrame:")
        print(df.info())

        # Convertir la columna 'fecha' a tipo datetime
        df['fecha'] = pd.to_datetime(df['fecha'])

    else:
        # La solicitud falló
        print(f"Error en la solicitud: {response.status_code}")
        print(response.text)

except requests.exceptions.RequestException as e:
    print(f"Error al realizar la solicitud: {e}")

print("\nADVERTENCIA: Esta solución desactiva la verificación SSL y no debe usarse en un entorno de producción sin entender los riesgos de seguridad asociados.")
print(df)
