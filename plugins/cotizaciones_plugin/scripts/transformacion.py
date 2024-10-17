import pandas as pd

def transform_data(accumulated_data):
    if accumulated_data:
        all_data = []

        for item in accumulated_data:
            fecha = item.get('fecha')
            detalles = item.get('detalle', [])
            for detalle in detalles:
                detalle['fecha'] = fecha
                all_data.append(detalle)


        df = pd.DataFrame(all_data)

        print("Estructura del DataFrame:", df.head())

        # Reordena columnas y cambia títulos
        if 'codigoMoneda' in df.columns and 'tipoCotizacion' in df.columns:

            df = df[['fecha', 'codigoMoneda', 'tipoCotizacion']]
            df.columns = ['FECHA', 'CÓDIGO MONEDA', 'COTIZACIÓN']

            # Convertir la columna 'FECHA' a formato de fecha
            df['FECHA'] = pd.to_datetime(df['FECHA'])

            # Filtrar para eliminar las filas donde 'CÓDIGO MONEDA' sea 'ARS', 'XAG', 'XAU'
            df = df[~df['CÓDIGO MONEDA'].isin(['ARS', 'XAG', 'XAU'])]

            # Convertir las columnas 'FECHA' de Timestamp a string
            df['FECHA'] = df['FECHA'].dt.strftime('%Y-%m-%d')

            return df  # Devuelve el DataFrame editado
        else:
            print("Error: Las columnas esperadas no están en el DataFrame.")
            return None
    else:
        print("Error: No se encontraron datos para transformar.")
        return None

