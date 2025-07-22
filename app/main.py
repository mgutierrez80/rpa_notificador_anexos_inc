import pandas as pd
import logging
from configparser import ConfigParser

from utils.redmineconnect import RedmineConnector
from utils.functions import process_incidents, generate_reports
from utils.emailsender import send_reports

# --- 1. CONFIGURACIÓN INICIAL ---
config = ConfigParser()
config.read("config/config.ini")

NOMBRE_CHECKLIST = config.get("Archivos", "checklist")
RUTA_REPORTES = config.get("Archivos", "ruta_reportes")


def main_job():
    """
    Función principal que orquesta todo el proceso de RPA.
    """
    logging.info("=====================================================")
    logging.info("INICIANDO PROCESO DE VERIFICACIÓN DE ANEXOS DE REDMINE")
    logging.info("=====================================================")

    try:
        # --- 2. EXTRACCIÓN DE DATOS DE REDMINE (EN PARALELO) ---
        logging.info("Paso 1: Extrayendo incidencias de Redmine en paralelo...")
        try:
            redmine_conn = RedmineConnector(config)
            issues_data = redmine_conn.get_redmine_issues_parallel()
        except Exception as e:
            logging.error(
                "No se pudo establecer la conexión inicial con Redmine. Finalizando proceso."
            )
            return

        if issues_data is None:
            logging.error(
                "La extracción de datos de Redmine falló. Revise los logs. Finalizando proceso."
            )
            return

        if not issues_data:
            logging.warning(
                "No se encontraron incidencias para el mes en curso. Finalizando proceso."
            )
            return

        df_incidencias = pd.DataFrame(issues_data)
        logging.info(
            f"Se construyó el DataFrame con {len(df_incidencias)} incidencias."
        )
        # Solo para pruebas, comentar en producción
        df_incidencias.to_excel("incidencias_redmine.xlsx", index=False)

        # --- 3. PROCESAMIENTO DE DATOS ---

        logging.info("Paso 2: Procesando incidencias y verificando anexos...")
        df_reporte_completo = process_incidents(df_incidencias, NOMBRE_CHECKLIST)
        df_reporte_completo.to_excel(
            "df_reporte_completo_incidencias_redmine.xlsx", index=False
        )
        if df_reporte_completo.empty:
            logging.warning(
                "El DataFrame procesado está vacío. No se generarán reportes."
            )
            return

        logging.info("Procesamiento completado.")

        # --- 4. GENERACIÓN DE REPORTES POR ZONA ---
        logging.info("Paso 3: Generando reportes por zona...")
        reportes_generados = generate_reports(df_reporte_completo, RUTA_REPORTES)

        if not reportes_generados:
            logging.warning("No se generaron reportes.")
            return

        logging.info(f"Se generaron {len(reportes_generados)} reportes.")

        # --- 5. ENVÍO DE CORREOS ---
        logging.info("Paso 4: Enviando reportes por correo electrónico...")
        send_reports(reportes_generados, config)

        logging.info("===================================================")
        logging.info("PROCESO FINALIZADO CON ÉXITO")
        logging.info("===================================================")

    except Exception as e:
        logging.error(
            f"Ocurrió un error inesperado en el proceso principal: {e}", exc_info=True
        )
        logging.info("===================================================")
        logging.info("PROCESO FINALIZADO CON ERRORES")
        logging.info("===================================================")
