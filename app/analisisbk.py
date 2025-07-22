import pandas as pd
import re
import os

# --- 1. CONFIGURACIÓN ---
# ¡IMPORTANTE! Revisa que estos nombres coincidan con tus archivos y columnas.
NOMBRE_ARCHIVO_EXCEL = r"C:\Users\gutie\OneDrive\Desktop\LIBROS INCIDENCIAS JUNIO\LISTADO_REDMINE_INCIDENCIAS_OESTE_JUNIO.xlsx"
NOMBRE_CHECKLIST = (
    r"C:\Users\gutie\Documents\PROYECTOS_ISES_2025\creacion_libros\ANEXOS_CHECKLIST.txt"
)
NOMBRE_REPORTE_SALIDA = "reporte_verificacion_anexos_METRO1607.xlsx"

# Nombres de las columnas en tu archivo Excel
COLUMNA_INCIDENCIA = "Incidencia"  # o 'ID', 'Incidencia', etc.
COLUMNA_ZONA = "Zona"
COLUMNA_TIPO_CAUSA = "Tipo de causa"
COLUMNA_CAUSA = "Causa"
COLUMNA_FICHEROS = "Ficheros"

# Ficheros especiales que requieren conteo de páginas
FICHEROS_CON_PAGINAS = [
    "Fotografias Certificadas",
    "Grabaciones de Voz o video",
    "Anexo de Video",
]


def leer_checklist(nombre_archivo):
    """Lee el archivo de checklist y devuelve una lista limpia de anexos."""
    try:
        with open(nombre_archivo, "r", encoding="utf-8") as f:
            anexos = [line.strip() for line in f if line.strip()]
        print(f"✅ Checklist '{nombre_archivo}' leído con {len(anexos)} anexos.")
        return anexos
    except FileNotFoundError:
        print(
            f"❌ Error: No se pudo encontrar el archivo de checklist '{nombre_archivo}'."
        )
        return None


def procesar_incidencias(df, checklist):
    """
    Procesa cada fila del DataFrame, verifica los anexos y genera los datos del reporte.
    """
    lista_reporte = []
    columnas_info = [
        COLUMNA_INCIDENCIA,
        COLUMNA_ZONA,
        COLUMNA_TIPO_CAUSA,
        COLUMNA_CAUSA,
    ]
    total_filas = len(df)

    for index, row in df.iterrows():
        print(f"Procesando incidencia {index + 1}/{total_filas}...", end="\r")

        # Inicia el diccionario de la fila del reporte con los datos de las columnas de información.
        fila_reporte = {col: row[col] for col in columnas_info}

        ficheros_texto = str(
            row[COLUMNA_FICHEROS]
        ).lower()  # Convertir a texto y minúsculas

        # Itera sobre cada anexo requerido en el checklist para verificar su existencia.
        for anexo_requerido in checklist:
            nombre_anexo_base = os.path.splitext(anexo_requerido)[0].lower()

            if nombre_anexo_base in ficheros_texto:
                fila_reporte[anexo_requerido] = 1
            else:
                fila_reporte[anexo_requerido] = 0

        lista_reporte.append(fila_reporte)

    print("\nProcesamiento completado.")

    # Asegurar el orden de las columnas en el reporte final
    columnas_finales = columnas_info + checklist
    df_final = pd.DataFrame(lista_reporte, columns=columnas_finales)

    return df_final


# --- 2. EJECUCIÓN PRINCIPAL ---
if __name__ == "__main__":
    print("Iniciando script de verificación de anexos (versión final)...")

    anexos_requeridos = leer_checklist(NOMBRE_CHECKLIST)

    if anexos_requeridos:
        try:
            df_incidencias = pd.read_excel(NOMBRE_ARCHIVO_EXCEL)
            print(f"✅ Archivo Excel '{NOMBRE_ARCHIVO_EXCEL}' leído correctamente.")

            df_reporte = procesar_incidencias(df_incidencias, anexos_requeridos)

            df_reporte.to_excel(NOMBRE_REPORTE_SALIDA, index=False)
            print(f"🎉 ¡Reporte final generado con éxito!")
            print(f"El archivo se ha guardado como: '{NOMBRE_REPORTE_SALIDA}'")

        except FileNotFoundError:
            print(
                f"❌ Error: No se pudo encontrar el archivo Excel '{NOMBRE_ARCHIVO_EXCEL}'."
            )
        except KeyError as e:
            print(
                f"❌ Error: La columna {e} no se encuentra en el archivo Excel. Revisa la sección de CONFIGURACIÓN."
            )
        except Exception as e:
            print(f"❌ Ocurrió un error inesperado: {e}")
    print("Iniciando script de verificación de anexos (versión corregida)...")

    # Cargar los datos
    anexos_requeridos = leer_checklist(NOMBRE_CHECKLIST)

    if anexos_requeridos:
        try:
            # Cambiado a pd.read_excel para leer archivos .xlsx
            df_incidencias = pd.read_excel(NOMBRE_ARCHIVO_EXCEL)
            print(f"✅ Archivo Excel '{NOMBRE_ARCHIVO_EXCEL}' leído correctamente.")

            # Procesar los datos
            df_reporte = procesar_incidencias(df_incidencias, anexos_requeridos)

            # Guardar el reporte final
            df_reporte.to_excel(NOMBRE_REPORTE_SALIDA, index=False)
            print(f"🎉 ¡Reporte final generado con éxito!")
            print(f"El archivo se ha guardado como: '{NOMBRE_REPORTE_SALIDA}'")

        except FileNotFoundError:
            print(
                f"❌ Error: No se pudo encontrar el archivo Excel '{NOMBRE_ARCHIVO_EXCEL}'."
            )
        except KeyError as e:
            print(
                f"❌ Error: La columna {e} no se encuentra en el archivo Excel. Revisa la sección de CONFIGURACIÓN."
            )
        except Exception as e:
            print(f"❌ Ocurrió un error inesperado: {e}")
