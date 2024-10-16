import pandas as pd

def transform_data(accumulated_data):
    if accumulated_data:
        # Crear una lista vacía para acumular los resultados
        all_data = []

        for item in accumulated_data:
            # Extraer fecha y detalles de las cotizaciones
            fecha = item.get('fecha')
            detalles = item.get('detalle', [])
            for detalle in detalles:
                detalle['fecha'] = fecha  # Añadir la fecha a cada detalle
                all_data.append(detalle)  # Agregar el detalle a la lista

        # Crear un DataFrame de pandas
        df = pd.DataFrame(all_data)

        # Imprimir la estructura del DataFrame para depuración
        print("Estructura del DataFrame:", df.head())

        # Asegurarse de que las columnas existen antes de seleccionar
        if 'codigoMoneda' in df.columns and 'tipoCotizacion' in df.columns:
            # Reordenar las columnas y poner los títulos en mayúsculas
            df = df[['fecha', 'codigoMoneda', 'tipoCotizacion']]
            df.columns = ['FECHA', 'CÓDIGO MONEDA', 'COTIZACIÓN']

            # Convertir la columna 'FECHA' a formato de fecha
            df['FECHA'] = pd.to_datetime(df['FECHA'])

            # Filtrar para eliminar las filas donde 'CÓDIGO MONEDA' sea 'ARS', 'XAG', 'XAU'
            df = df[~df['CÓDIGO MONEDA'].isin(['ARS', 'XAG', 'XAU'])]

            # Convertir las columnas 'FECHA' de Timestamp a string
            df['FECHA'] = df['FECHA'].dt.strftime('%Y-%m-%d')

            return df  # Devolver el DataFrame procesado
        else:
            print("Error: Las columnas esperadas no están en el DataFrame.")
            return None
    else:
        print("Error: No se encontraron datos para transformar.")
        return None

