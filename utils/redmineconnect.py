from redminelib import Redmine
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import os


class RedmineConnector:
    """
    Clase para gestionar la conexión y extracción de datos de Redmine,
    optimizada con ThreadPoolExecutor para concurrencia I/O.
    """

    def __init__(self, config):
        """
        Inicializa la conexión a Redmine y carga la configuración.
        """
        try:
            self.url = config.get("Redmine", "url")
            self.api_key = config.get("Redmine", "api_key")
            self.headers = {
                "X-Redmine-API-Key": self.api_key,
                "Content-Type": "application/json",
            }
            # Objeto Redmine principal para la llamada inicial
            self.redmine = Redmine(
                self.url,
                key=self.api_key,
                requests={"verify": False, "headers": self.headers},
            )
            self.redmine.auth()

            self.project_id = config.get("Redmine", "project_id")

            self.maps_dict = {
                "incidencia": int(config.get("MapeoCamposRedmine", "incidencia")),
                "fecha_incidencia": int(
                    config.get("MapeoCamposRedmine", "fecha_incidencia")
                ),
                "zona": int(config.get("MapeoCamposRedmine", "zona")),
                "causa": int(config.get("MapeoCamposRedmine", "causa")),
                "tipo_causa": int(config.get("MapeoCamposRedmine", "tipo_causa")),
            }

            logging.info("Conexión con Redmine establecida y configuración cargada.")
        except Exception as e:
            logging.error(f"Error al inicializar RedmineConnector: {e}", exc_info=True)
            raise

    def _fetch_page(self, offset, limit, start_date):
        """
        Función trabajadora que obtiene una página de incidencias.
        Es un método de la clase para acceder a la configuración fácilmente.
        """
        today = datetime.now()
        start_date = today.replace(day=1).strftime("%Y-%m-%d")
        end_date = today.strftime("%Y-%m-%d")
        filtro_fecha = f"><{start_date}|{end_date}"
        processed_issues = []
        try:
            # Se usa el objeto Redmine de la instancia. redminelib es thread-safe para lecturas.
            issues = self.redmine.issue.filter(
                project_id=self.project_id,
                created_on=filtro_fecha,
                cf_21=filtro_fecha,  # Asumiendo que cf_21 es el campo de fecha de incidencia
                status_id=5,  # Asumiendo que el estado 5 es "Cerrado"
                cf_18="PROCEDE",
                include=["attachments"],
                sort="updated_on:asc",
            )

            def get_cf_value(issue, cf_id):
                try:
                    return next(
                        (cf.value for cf in issue.custom_fields if cf.id == cf_id), None
                    )
                except (StopIteration, AttributeError):
                    return None

            for issue in issues:
                attachments = [
                    att.filename
                    for att in issue.attachments
                    if att.filename.lower().endswith(".pdf")
                ]

                issue_dict = {
                    "Ticket": issue.id,
                    "Asunto": issue.subject,
                    "Incidencia": get_cf_value(issue, self.maps_dict["incidencia"]),
                    "Fecha Incidencia": get_cf_value(
                        issue, self.maps_dict["fecha_incidencia"]
                    ),
                    "Zona": get_cf_value(issue, self.maps_dict["zona"]),
                    "Causa": get_cf_value(issue, self.maps_dict["causa"]),
                    "Tipo de causa": get_cf_value(issue, self.maps_dict["tipo_causa"]),
                    "Ficheros": ", ".join(attachments),
                }
                processed_issues.append(issue_dict)
        except Exception as e:
            logging.error(
                f"Error en el hilo trabajador para el offset {offset}: {e}",
                exc_info=True,
            )

        return processed_issues

    def get_redmine_issues_parallel(self):
        """
        Extrae las incidencias del mes en curso desde Redmine usando un pool de hilos.
        """
        try:
            limit = 100
            today = datetime.now()
            start_date = today.replace(day=1).strftime("%Y-%m-%d")
            end_date = today.strftime("%Y-%m-%d")
            filtro_fecha = f"><{start_date}|{end_date}"

            # 1. Llamada inicial para obtener el conteo total (necesario para la paginación)
            resource_set = self.redmine.issue.filter(
                project_id=self.project_id, created_on=filtro_fecha, limit=1
            )
            len(resource_set)
            total_count = resource_set.total_count
            logging.info(
                f"Total de incidencias encontradas para el mes en curso: {total_count}"
            )

            if total_count == 0:
                return []

            all_issues_data = []
            # Usar un número razonable de hilos. os.cpu_count() * 5 es un buen punto de partida para I/O.
            max_workers = min(32, (os.cpu_count() or 1) * 5)

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 2. Crear y enviar todas las tareas al pool
                future_to_offset = {
                    executor.submit(self._fetch_page, offset, limit, start_date): offset
                    for offset in range(0, total_count, limit)
                }

                logging.info(
                    f"Iniciando pool con hasta {max_workers} hilos para la extracción de {len(future_to_offset)} páginas."
                )

                # 3. Recolectar los resultados a medida que se completan
                for future in as_completed(future_to_offset):
                    offset = future_to_offset[future]
                    try:
                        page_data = future.result()
                        if page_data:
                            all_issues_data.extend(page_data)
                    except Exception as exc:
                        logging.error(
                            f"La página con offset {offset} generó una excepción: {exc}"
                        )
            unique_issues = []
            seen_tickets = set()
            for issue in all_issues_data:
                ticket_id = issue.get("Ticket")
                if ticket_id and ticket_id not in seen_tickets:
                    unique_issues.append(issue)
                    seen_tickets.add(ticket_id)

            # Informar si se encontraron y eliminaron duplicados
            if len(all_issues_data) != len(unique_issues):
                logging.warning(
                    f"Se encontraron y eliminaron {len(all_issues_data) - len(unique_issues)} incidencias duplicadas durante la extracción."
                )
            logging.info(
                f"Extracción concurrente completada. Se procesaron {len(all_issues_data)} de {total_count} incidencias."
            )

            return unique_issues

        except Exception as e:
            logging.error(
                f"Error al extraer incidencias de Redmine en paralelo: {e}",
                exc_info=True,
            )
            return None
