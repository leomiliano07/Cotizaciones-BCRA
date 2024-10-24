import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import sys
# Simula las dependencias de Airflow
sys.modules['airflow'] = MagicMock()
sys.modules['airflow.hooks.base_hook'] = MagicMock()
from plugins.cotizaciones_plugin.scripts.carga import load_to_redshift

class TestLoad(unittest.TestCase):

    @patch('plugins.cotizaciones_plugin.scripts.carga.psycopg2.connect')
    def test_load_to_redshift(self, mock_connect):
        # Configura el DataFrame de prueba
        df = pd.DataFrame({
            'FECHA': ['2024-10-16'],
            'CÓDIGO MONEDA': ['USD'],
            'COTIZACIÓN': [151.0]
        })

        # Llama a la función con datos simulados
        load_to_redshift(df, 'fake_host', 'fake_port', 'fake_db', 'fake_user', 'fake_password', 'fake_schema', 'fake_table')

        # Verifica que la conexión fue llamada
        mock_connect.assert_called_once()

if __name__ == '__main__':
    unittest.main()