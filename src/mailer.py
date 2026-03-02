"""
Mailer Module.

Handles data processing, Excel generation in memory, and sending emails via SMTP.
Implements type safety and robust error handling.
"""

import os
import io
import smtplib
import logging
from typing import List, Dict, Any, Optional
from datetime import date

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

import pandas as pd
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv()

class ReportMailer:
    """
    Manages the generation of Excel reports and email transmission.
    """

    @staticmethod
    def _format_date_mx(date_obj: Optional[date]) -> str:
        """Formats a date object to 'DD/MM/YYYY'."""
        if date_obj:
            return date_obj.strftime('%d/%m/%Y')
        return "N/A"

    @staticmethod
    def _get_status_guesty(raw_val: Any) -> str:
        """Converts raw JSON boolean/string value to readable status."""
        val = str(raw_val).strip().lower()
        if val == 'true': return "ACTIVE"
        if val == 'false': return "INACTIVE"
        return "N/A"

    @classmethod
    def _create_property_dict(cls, row: Dict[str, Any]) -> Dict[str, str]:
        """Creates a dictionary for Sheet 1: Property Inventory."""
        return {
            "COUNTRY": str(row.get('country', 'N/A')),
            "PROPERTY": str(row.get('property', 'N/A')),
            "OFFBOARDING GUESTY": cls._format_date_mx(row.get('offboarding_date')),
            "STATUS GUESTY": cls._get_status_guesty(row.get('status_json'))
        }

    @classmethod
    def _create_reservation_dict(cls, row: Dict[str, Any], is_violation: bool = True) -> Dict[str, str]:
        """Creates a dictionary for Sheets 2 and 3: Reservation Details."""
        status_icon = "❌ ALERTA" if is_violation else "✅ OK"
        return {
            "COUNTRY": str(row.get('country', 'N/A')),
            "PROPERTY": str(row.get('property', 'N/A')),
            "CONFIRMATION CODE": str(row.get('confirmation_code', 'N/A')),
            "OFFBOARDING GUESTY": cls._format_date_mx(row.get('offboarding_date')),
            "CHECK IN": cls._format_date_mx(row.get('check_in_date')),
            "CHECK OUT": cls._format_date_mx(row.get('check_out_date')),
            "STATUS": status_icon,
            "STATUS GUESTY": cls._get_status_guesty(row.get('status_json'))
        }

    @classmethod
    def generate_excel(
        cls, 
        proactive_data: List[Dict[str, Any]], 
        alerts_data: List[Dict[str, Any]], 
        reactive_data: List[Dict[str, Any]]
    ) -> bytes:
        """Generates the Excel file in memory."""
        
        # Prepare data
        list_proactive = [cls._create_property_dict(row) for row in proactive_data]
        list_alerts = [cls._create_reservation_dict(row) for row in alerts_data]
        list_reactive = [cls._create_reservation_dict(row) for row in reactive_data]

        # Define headers
        cols_properties = ["COUNTRY", "PROPERTY", "OFFBOARDING GUESTY", "STATUS GUESTY"]
        cols_reservations = ["COUNTRY", "PROPERTY", "CONFIRMATION CODE", "OFFBOARDING GUESTY", "CHECK IN", "CHECK OUT", "STATUS", "STATUS GUESTY"]

        # Create DataFrames
        df_proactive = pd.DataFrame(list_proactive, columns=cols_properties)
        df_alerts = pd.DataFrame(list_alerts, columns=cols_reservations)
        df_reactive = pd.DataFrame(list_reactive, columns=cols_reservations)

        # Write to buffer
        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            df_proactive.to_excel(writer, sheet_name='Inventario Deptos', index=False)
            df_alerts.to_excel(writer, sheet_name='Alertas Activas', index=False)
            df_reactive.to_excel(writer, sheet_name='Alertas Históricas', index=False)
        
        return excel_buffer.getvalue()

    @staticmethod
    def send_email(
        subject: str, 
        body_html: str, 
        attachment_bytes: bytes, 
        filename: str
    ) -> None:
        """Sends the email with attachment."""
        sender = os.getenv("EMAIL_SENDER")
        password = os.getenv("EMAIL_PASSWORD")
        
        # Get recipients
        cx_emails = os.getenv("EMAIL_CX", "")
        rodrigo_emails = os.getenv("EMAIL_RODRIGO", "")
        recipients_str = f"{cx_emails},{rodrigo_emails}"
        recipients = [email.strip() for email in recipients_str.split(",") if email.strip()]

        if not recipients:
            logger.warning("⚠️ No recipients defined. Email not sent.")
            return

        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = sender
        msg['To'] = ", ".join(recipients)

        msg.attach(MIMEText(body_html, 'html'))

        part = MIMEBase('application', 'vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        part.set_payload(attachment_bytes)
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="{filename}"')
        msg.attach(part)

        try:
            # Using SMTP_SSL for port 465
            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
                server.login(sender, password)
                server.sendmail(sender, recipients, msg.as_string())
            logger.info("✅ Email sent successfully.")
        except Exception as e:
            logger.error(f"❌ Failed to send email: {e}")

def send_alert_email(
    proactive_data: List[Dict[str, Any]], 
    alerts_data: List[Dict[str, Any]], 
    reactive_data: List[Dict[str, Any]]
) -> None:
    """
    Orchestrator function to generate report and send email.
    """
    if not proactive_data and not alerts_data and not reactive_data: 
        logger.info("ℹ️ No data to report.")
        return

    # Generate Excel
    try:
        excel_bytes = ReportMailer.generate_excel(proactive_data, alerts_data, reactive_data)
    except Exception as e:
        logger.error(f"❌ Error generating Excel: {e}")
        return

    # Prepare Email Content
    active_count = len(alerts_data)
    reactive_count = len(reactive_data)
    
    subject = f"📊 Reporte de Alertas Offboarding: {active_count} Activas / {reactive_count} Históricas"

    # HTMl Body with inline styles for better readability in email clients
    html_body = f"""
    <html>
    <body style="font-family: Arial, sans-serif; color: #333; font-size: 14px; line-height: 1.5;">
        <div style="max-width: 650px; padding: 25px; border: 1px solid #e0e0e0; border-radius: 8px; box-shadow: 0 4px 8px rgba(0,0,0,0.05);">
            <h2 style="color: #1565C0; margin-top: 0;">📅 Reporte de Alertas: Offboarding</h2>
            <p>¡Hola equipo!</p>
            <p>Espero que estén teniendo un excelente día. Aquí les envío el reporte con los departamentos y las alertas de reservas detectadas fuera de fecha.</p>
            
            <div style="background-color: #FFF3F3; padding: 15px; border-left: 5px solid #D32F2F; margin: 20px 0; border-radius: 4px;">
                <h3 style="margin-top: 0; color: #D32F2F;">Resumen de Alertas (Check-Out > Offboarding):</h3>
                <ul style="margin-bottom: 0;">
                    <li><b>🚨 Alertas Activas (Departamentos Activos):</b> {active_count} reservas detectadas.</li>
                    <li><b>🗄️ Alertas Históricas (Departamentos Inactivos):</b> {reactive_count} reservas registradas.</li>
                </ul>
            </div>
            
            <p>En el archivo de Excel adjunto encontrarán las siguientes pestañas a detalle:</p>
            <ul style="padding-left: 20px;">
                <li><b>🏢 Inventario Deptos:</b> Lista de los departamentos (activos) que tienen fecha de Blockoff.</li>
                <li><b>🚨 Alertas Activas:</b> El detalle de las reservas críticas que debemos atender.</li>
                <li><b>🗄️ Alertas Históricas:</b> El registro de incidencias pasadas.</li>
            </ul>
            <p style="margin-top: 30px;">Saludos</p>
        </div>
    </body>
    </html>
    """

    # Send Email
    ReportMailer.send_email(subject, html_body, excel_bytes, "Alertas_Offboarding.xlsx")