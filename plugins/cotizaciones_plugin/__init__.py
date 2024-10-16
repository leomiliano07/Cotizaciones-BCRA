from airflow.plugins_manager import AirflowPlugin
from cotizaciones_plugin.scripts import extraccion, transformacion, carga

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