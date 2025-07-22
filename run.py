import schedule
import time
import logging
import sys
import os

# Agrega el directorio raíz al path para asegurar que las importaciones funcionen
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.main import main_job
from config.logger import setup_logging

# --- PROGRAMACIÓN DE LA TAREA ---
if __name__ == "__main__":
    # Esta protección es VITAL para el multiprocesamiento en Windows.

    # 1. Configurar logging
    setup_logging()

    # 2. Programar el job
    logging.info("Servicio de RPA iniciado. Esperando la hora programada...")

    # Descomentar para pruebas inmediatas
    main_job()

    schedule.every().monday.at("16:30").do(main_job)
    schedule.every().wednesday.at("16:30").do(main_job)
    schedule.every().friday.at("16:30").do(main_job)

    # 3. Bucle de ejecución
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)  # Espera 60 segundos antes de volver a verificar
        except KeyboardInterrupt:
            logging.info("Proceso de RPA detenido manualmente.")
            break
        except Exception as e:
            logging.error(
                f"Error en el bucle principal del scheduler: {e}", exc_info=True
            )
            time.sleep(300)  # Espera 5 minutos antes de reintentar en caso de error
