from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import sys
# Simula las dependencias de Airflow
sys.modules['airflow'] = MagicMock()
sys.modules['airflow.hooks.base_hook'] = MagicMock()
from plugins.cotizaciones_plugin.scripts.extraccion import extract_data

@patch('plugins.cotizaciones_plugin.scripts.extraccion.BaseHook.get_connection')
@patch('plugins.cotizaciones_plugin.scripts.extraccion.requests.get')
@patch('plugins.cotizaciones_plugin.scripts.extraccion.get_last_loaded_date')
def test_extract_data(mock_get_last_loaded_date, mock_get, mock_get_connection):
    # Simula la respuesta de la API de BCRA
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "results": [
            {"fecha": "2024-10-11", "moneda": "USD", "cotizacion": 340},
        ]
    }
    mock_get.return_value = mock_response

    # Simula la función para extraer la última fecha cargada
    mock_get_last_loaded_date.return_value = datetime(2024, 10, 10).date()

    # Simula la conexión a Redshift
    mock_connection = Mock()
    mock_get_connection.return_value = mock_connection

    # Ejecuta el código de prueba
    result = extract_data()
    print("Resultado de extract_data():", result)

    # Validaciones
    assert result is not None, "El resultado no debería ser None."
    assert isinstance(result, list) and len(result) > 0, "El resultado debería ser una lista con elementos."
    assert isinstance(result[0], list) and len(result[0]) > 0, "El primer elemento debería ser una lista con elementos."
    assert 'cotizacion' in result[0][0], "El primer elemento debería tener la clave 'cotizacion'."
    assert result[0][0]['cotizacion'] == 340, "La cotización no coincide."