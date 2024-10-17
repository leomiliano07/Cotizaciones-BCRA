import sys
import os
import unittest

print("Iniciando pruebas...")

# Agrega el directorio principal del proyecto a PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'plugins')))

# Descubrir y ejecutar las pruebas
loader = unittest.TestLoader()
tests = loader.discover('tests')
test_runner = unittest.TextTestRunner()
print("Ejecutando pruebas...")
result = test_runner.run(tests)

if result.wasSuccessful():
    print("Todas las pruebas pasaron.")
else:
    print("Algunas pruebas fallaron.")
