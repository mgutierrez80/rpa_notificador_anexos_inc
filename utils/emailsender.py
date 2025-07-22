import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import json
import logging
import os


def send_reports(reportes_generados, config):
    """
    Envía los reportes generados a los correos correspondientes por zona.
    """
    try:
        with open("config/email_map.json", "r", encoding="utf-8") as f:
            email_map = json.load(f)
    except FileNotFoundError:
        logging.error(
            "No se encontró el archivo 'config/email_map.json'. No se enviarán correos."
        )
        return
    except json.JSONDecodeError:
        logging.error(
            "Error al decodificar 'config/email_map.json'. Verifique el formato."
        )
        return

    sender_email = config.get("Email", "sender_email")
    sender_password = config.get("Email", "sender_password")
    smtp_server = config.get("Email", "smtp_server")
    smtp_port = int(config.get("Email", "smtp_port"))
    subject_prefix = config.get("Email", "subject_prefix")

    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(sender_email, sender_password)
        logging.info("Conexión exitosa con el servidor SMTP.")

        for zona, ruta_reporte in reportes_generados.items():
            if zona in email_map:
                destinatarios = email_map[zona]

                msg = MIMEMultipart()
                msg["From"] = sender_email
                msg["To"] = ", ".join(destinatarios)
                msg["Subject"] = f"{subject_prefix} - {zona}"

                body = f"""
                <html>
                <body>
                <p>Estimados,</p>
                <p>Se adjunta el reporte de verificación de anexos para la <b>zona {zona}</b>.</p>
                <p>Este es un correo generado automáticamente por el sistema de RPA.</p>
                <p>Saludos cordiales.</p>
                </body>
                </html>
                """
                msg.attach(MIMEText(body, "html"))

                with open(ruta_reporte, "rb") as attachment:
                    part = MIMEBase("application", "octet-stream")
                    part.set_payload(attachment.read())

                encoders.encode_base64(part)
                part.add_header(
                    "Content-Disposition",
                    f"attachment; filename= {os.path.basename(ruta_reporte)}",
                )
                msg.attach(part)

                server.sendmail(sender_email, destinatarios, msg.as_string())
                logging.info(
                    f"Correo para la zona '{zona}' enviado a: {', '.join(destinatarios)}"
                )
            else:
                logging.warning(
                    f"No se encontró mapeo de correo para la zona '{zona}'. No se enviará el reporte."
                )

        server.quit()
        logging.info("Conexión con el servidor SMTP cerrada.")

    except Exception as e:
        logging.error(f"Error al enviar correos electrónicos: {e}", exc_info=True)
