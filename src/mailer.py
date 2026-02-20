import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from dotenv import load_dotenv
import logging
import pandas as pd
import io

logger = logging.getLogger(__name__)
load_dotenv()

def format_date_mx(date_obj):
    if date_obj: return date_obj.strftime('%d/%m/%Y')
    return "N/A"

def create_row_dict(row, is_violation=False):
    """Ayuda a crear un diccionario para Pandas (Filas de Excel)"""
    off_date = row.get('offboarding_date')
    check_in = row.get('check_in_date')
    check_out = row.get('check_out_date')
    
    # --- TRADUCCIÓN DE STATUS GUESTY (True/False -> Active/Inactive) ---
    raw_guesty = str(row.get('status_json', '')).strip().lower()
    if raw_guesty == 'true':
        status_guesty = "ACTIVE"
    elif raw_guesty == 'false':
        status_guesty = "INACTIVE"
    else:
        status_guesty = "N/A"

    status_icon = "❌ ALERTA" if is_violation else "✅ OK"

    # Retorna un diccionario con las columnas exactas que queremos en el Excel
    return {
        "COUNTRY": row.get('country', 'N/A'),
        "PROPERTY": row.get('property', 'N/A'),
        "CONFIRMATION CODE": row.get('confirmation_code', 'N/A'),
        "OFFBOARDING GUESTY": format_date_mx(off_date),
        "CHECK IN": format_date_mx(check_in),
        "CHECK OUT": format_date_mx(check_out),
        "STATUS": status_icon,
        "STATUS GUESTY": status_guesty
    }

def send_alert_email(proactive_data, reactive_data):
    if not proactive_data and not reactive_data: 
        logger.info("ℹ️ No hay datos para reportar.")
        return

    sender = os.getenv("EMAIL_SENDER")
    password = os.getenv("EMAIL_PASSWORD")
    cx_emails = os.getenv("EMAIL_CX", "")
    rodrigo_emails = os.getenv("EMAIL_RODRIGO", "")
    all_emails_string = f"{cx_emails},{rodrigo_emails}"
    recipients = [email.strip() for email in all_emails_string.split(",") if email.strip()]

    if not recipients: return

    # --- 1. PREPARAR DATOS PARA EL EXCEL ---
    list_proactive = []
    list_alerts = []
    current_alert_count = 0

    if proactive_data:
        for row in proactive_data:
            off = row.get('offboarding_date')
            out = row.get('check_out_date')
            is_bad = bool(out and off and out > off)

            row_dict = create_row_dict(row, is_bad)
            list_proactive.append(row_dict)
            
            if is_bad:
                list_alerts.append(row_dict)
                current_alert_count += 1

    list_reactive = []
    reactive_count = 0
    if reactive_data:
        for row in reactive_data:
            list_reactive.append(create_row_dict(row, is_violation=True))
            reactive_count += 1

    # Definir columnas para asegurar el formato incluso si las listas están vacías
    columns = ["COUNTRY", "PROPERTY", "CONFIRMATION CODE", "OFFBOARDING GUESTY", "CHECK IN", "CHECK OUT", "STATUS", "STATUS GUESTY"]
    
    df_proactive = pd.DataFrame(list_proactive, columns=columns)
    df_alerts = pd.DataFrame(list_alerts, columns=columns)
    df_reactive = pd.DataFrame(list_reactive, columns=columns)

    # --- 2. CREAR EXCEL EN MEMORIA (Buffer) ---
    excel_buffer = io.BytesIO()
    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
        df_proactive.to_excel(writer, sheet_name='Sistema Proactivo', index=False)
        df_alerts.to_excel(writer, sheet_name='Alertas Activas', index=False)
        df_reactive.to_excel(writer, sheet_name='Sistema Reactivo', index=False)
    
    # Obtener los bytes del archivo Excel
    excel_data = excel_buffer.getvalue()

    # --- 3. CONFIGURAR EL CORREO ---
    msg = MIMEMultipart()
    total_alerts = current_alert_count + reactive_count
    msg['Subject'] = f"📊 Reporte Offboarding Excel: {current_alert_count} Alertas Activas / {reactive_count} Históricas"
    msg['From'] = sender
    msg['To'] = ", ".join(recipients)

    # Cuerpo del correo en HTML
    html_body = f"""
    <html>
    <body style="font-family: Arial, sans-serif; color: #333; font-size: 14px;">
        <div style="max-width: 600px; padding: 20px; border: 1px solid #ccc; border-radius: 5px;">
            <h2 style="color: #1565C0;">📅 Reporte de Offboarding</h2>
            <p>Hola equipo,</p>
            <p>Se adjunta el archivo Excel con el reporte actualizado de reservas fuera de la fecha de offboarding.</p>
            
            <div style="background-color: #f5f5f5; padding: 15px; border-left: 4px solid #c62828; margin: 20px 0;">
                <h3 style="margin-top: 0;">Resumen de Alertas:</h3>
                <ul style="margin-bottom: 0;">
                    <li><b>Alertas Activas (Actuales):</b> {current_alert_count}</li>
                    <li><b>Alertas Reactivas (Históricas):</b> {reactive_count}</li>
                </ul>
            </div>
            
            <p>El archivo adjunto contiene 3 pestañas:</p>
            <ol>
                <li><b>Sistema Proactivo:</b> Todo el panorama actual (últimos 30 días y futuro).</li>
                <li><b>Alertas Activas:</b> Solo las reservas críticas filtradas.</li>
                <li><b>Sistema Reactivo:</b> Todo el historial de incidencias de la base de datos.</li>
            </ol>
            <p>Saludos.</p>
        </div>
    </body>
    </html>
    """
    msg.attach(MIMEText(html_body, 'html'))

    # --- 4. ADJUNTAR EL EXCEL AL CORREO ---
    part = MIMEBase('application', 'vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    part.set_payload(excel_data)
    encoders.encode_base64(part)
    # Nombre del archivo que verán los usuarios
    part.add_header('Content-Disposition', 'attachment; filename="Reporte_Offboarding.xlsx"')
    msg.attach(part)

    # --- 5. ENVIAR CORREO ---
    try:
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.login(sender, password)
        server.sendmail(sender, recipients, msg.as_string())
        server.quit()
        logger.info("✅ Reporte Excel (3 hojas) enviado con éxito.")
    except Exception as e:
        logger.error(f"❌ Falló envío: {e}")