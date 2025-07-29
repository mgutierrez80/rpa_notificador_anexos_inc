import pandas as pd
import os
import logging
from datetime import datetime, timedelta
import time


def leer_checklist(nombre_archivo):
    """
    Lee el archivo de checklist y devuelve una lista limpia de anexos.
    """
    try:
        with open(nombre_archivo, "r", encoding="utf-8") as f:
            anexos = [line.strip() for line in f if line.strip()]
        logging.info(f"Checklist '{nombre_archivo}' leído con {len(anexos)} anexos.")
        return anexos
    except FileNotFoundError:
        logging.error(
            f"No se pudo encontrar el archivo de checklist '{nombre_archivo}'."
        )
        return None


def process_incidents(df, checklist_path):
    """
    Procesa el DataFrame de incidencias para verificar los anexos.
    Reutiliza y mejora la lógica de 'analisis.py'.
    """
    checklist = leer_checklist(checklist_path)
    if not checklist:
        return pd.DataFrame()

    lista_reporte = []

    # Columnas obligatorias que deben existir en el DataFrame de Redmine
    columnas_obligatorias = [
        "Ticket",
        "Incidencia",
        "Fecha Incidencia",
        "Zona",
        "Asunto",
        "Ficheros",
        "Causa",
        "Tipo de causa",
    ]

    # Verificar que todas las columnas existan
    for col in columnas_obligatorias:
        if col not in df.columns:
            logging.error(
                f"La columna requerida '{col}' no se encontró en los datos de Redmine."
            )
            return pd.DataFrame()

    total_filas = len(df)
    logging.info(f"Iniciando procesamiento de {total_filas} incidencias.")

    for index, row in df.iterrows():
        fila_reporte = {
            "Ticket": row["Ticket"],
            "Incidencia": row["Incidencia"],
            "Fecha Incidencia": row["Fecha Incidencia"],
            "Zona": row["Zona"],
            "Asunto": row["Asunto"],
            "Causa": row["Causa"],
            "Tipo de causa": row["Tipo de causa"],
        }

        ficheros_texto = str(row["Ficheros"]).lower()

        for anexo_requerido in checklist:
            # Comparamos el nombre base del anexo (sin extensión)
            nombre_anexo_base = os.path.splitext(anexo_requerido)[0].lower()
            fila_reporte[anexo_requerido] = (
                1 if nombre_anexo_base in ficheros_texto else 0
            )

        lista_reporte.append(fila_reporte)

    columnas_finales = columnas_obligatorias[:5] + columnas_obligatorias[6:] + checklist
    df_final = pd.DataFrame(lista_reporte, columns=columnas_finales)

    # Renombrar columnas para el reporte final
    df_final = df_final.rename(columns={"Tipo de causa": "Tipo de Causa"})

    return df_final


def generate_reports(df_reporte, ruta_base):
    """
    Genera un archivo Excel de reporte por cada zona.
    """
    if not os.path.exists(ruta_base):
        os.makedirs(ruta_base)
        logging.info(f"Directorio de reportes creado en: {ruta_base}")

    zonas = df_reporte["Zona"].unique()
    reportes_generados = {}

    for zona in zonas:
        if pd.isna(zona):
            logging.warning("Se encontró una zona con valor Nulo. Se omitirá.")
            continue

        df_zona = df_reporte[df_reporte["Zona"] == zona]

        # Orden de columnas para el reporte final
        columnas_reporte = [
            "Ticket",
            "Incidencia",
            "Fecha Incidencia",
            "Zona",
            "Asunto",
            "Causa",
            "Tipo de Causa",
        ] + [
            col
            for col in df_zona.columns
            if col
            not in [
                "Ticket",
                "Incidencia",
                "Fecha Incidencia",
                "Zona",
                "Asunto",
                "Causa",
                "Tipo de Causa",
            ]
        ]

        df_zona = df_zona[columnas_reporte]

        fecha_actual = datetime.now().strftime("%Y%m%d")
        nombre_archivo = f"Reporte_Verificacion_{zona}_{fecha_actual}.xlsx"
        ruta_completa = os.path.join(ruta_base, nombre_archivo)

        try:
            df_zona.to_excel(ruta_completa, index=False)
            logging.info(f"Reporte para la zona '{zona}' generado en: {ruta_completa}")
            reportes_generados[zona] = ruta_completa
        except Exception as e:
            logging.error(f"No se pudo guardar el reporte para la zona '{zona}': {e}")

    return reportes_generados


def cleanup_old_reports(folder_path, days_to_keep):
    """
    Borra archivos en una carpeta específica que son más antiguos
    que un número de días determinado.

    Args:
        folder_path (str): La ruta a la carpeta que se va a limpiar.
        days_to_keep (int): El número máximo de días que un archivo puede tener.
    """
    if not os.path.isdir(folder_path):
        logging.warning(
            f"La carpeta de limpieza '{folder_path}' no existe. Se omitirá el paso."
        )
        return

    try:
        cutoff_time = time.time() - (days_to_keep * 24 * 60 * 60)
        files_deleted_count = 0

        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)

            # Asegurarse de que es un archivo y no una carpeta
            if os.path.isfile(file_path):
                file_mod_time = os.path.getmtime(file_path)
                if file_mod_time < cutoff_time:
                    try:
                        os.remove(file_path)
                        logging.info(f"Archivo antiguo eliminado: {filename}")
                        files_deleted_count += 1
                    except Exception as e:
                        logging.error(f"No se pudo eliminar el archivo {filename}: {e}")

        if files_deleted_count == 0:
            logging.info("No se encontraron archivos antiguos para eliminar.")
        else:
            logging.info(
                f"Limpieza completada. Se eliminaron {files_deleted_count} archivos."
            )

    except Exception as e:
        logging.error(
            f"Ocurrió un error durante la limpieza de archivos antiguos: {e}",
            exc_info=True,
        )
