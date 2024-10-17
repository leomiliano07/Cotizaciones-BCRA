try:
    from airflow.plugins_manager import AirflowPlugin
except ImportError:
    class AirflowPlugin:
        # Stub vacío para evitar la dependencia de Airflow
        pass

from .scripts import extraccion, transformacion, carga

class CotizacionesPlugin(AirflowPlugin):
    name = "cotizaciones_plugin"
    operators = []
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
    appbuilder_views = []
    appbuilder_menu_items = []
    global_operator_extra_links = []
    operator_extra_links = []
    
class CotizacionesPlugin(AirflowPlugin):
    name = "cotizaciones_plugin"
    operators = []
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
    appbuilder_views = []
    appbuilder_menu_items = []
    global_operator_extra_links = []
    operator_extra_links = []