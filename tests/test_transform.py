import unittest
from plugins.cotizaciones_plugin.scripts.transformacion import transform_data

class TestTransformation(unittest.TestCase):

    def test_transform_data(self):
        accumulated_data = [
            {'fecha': '2024-10-16', 'detalle': [{'codigoMoneda': 'USD', 'tipoCotizacion': 151.0}, {'codigoMoneda': 'ARS', 'tipoCotizacion': 200.0}]}
        ]
        
        df = transform_data(accumulated_data)

        # Solo debería quedar USD
        self.assertEqual(df.shape[0], 1)
        self.assertIn('FECHA', df.columns)
        self.assertIn('CÓDIGO MONEDA', df.columns)
        self.assertEqual(df.iloc[0]['CÓDIGO MONEDA'], 'USD')

    def test_transform_data_empty(self):
        accumulated_data = []
        df = transform_data(accumulated_data)
        # Debe devolver None
        self.assertIsNone(df)

    def test_transform_data_missing_columns(self):
        accumulated_data = [{'fecha': '2024-10-16', 'detalle': [{'codigoMoneda': 'USD'}]}]
        df = transform_data(accumulated_data)
        # Debe devolver None si faltan columnas clave
        self.assertIsNone(df)

    def test_transform_data_no_filtering_needed(self):
        accumulated_data = [
            {'fecha': '2024-10-16', 'detalle': [{'codigoMoneda': 'USD', 'tipoCotizacion': 151.0}, {'codigoMoneda': 'EUR', 'tipoCotizacion': 200.0}]}
        ]
        
        df = transform_data(accumulated_data)
        
        # Deben quedar ambas monedas
        self.assertEqual(df.shape[0], 2)
        self.assertIn('USD', df['CÓDIGO MONEDA'].values)
        self.assertIn('EUR', df['CÓDIGO MONEDA'].values)

    def test_transform_data_date_format(self):
        accumulated_data = [
            {'fecha': '2024-10-16', 'detalle': [{'codigoMoneda': 'USD', 'tipoCotizacion': 151.0}]}
        ]
        
        df = transform_data(accumulated_data)

        # Verifica que la fecha sea YYYY-MM-DD
        self.assertEqual(df.iloc[0]['FECHA'], '2024-10-16')

if __name__ == '__main__':
    unittest.main()