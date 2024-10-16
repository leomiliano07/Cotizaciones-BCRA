try:
    from .extraccion import extract_data
    from .transformacion import transform_data
    from .carga import load_to_redshift
except ImportError:
    from cotizaciones_plugin.scripts.extraccion import extract_data
    from cotizaciones_plugin.scripts.transformacion import transform_data
    from cotizaciones_plugin.scripts.carga import load_to_redshift