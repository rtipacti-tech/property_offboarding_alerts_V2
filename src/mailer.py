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

def get_status_guesty(raw_val):
    """Convierte el JSON true/false a ACTIVE/INACTIVE"""
    val = str(raw_val).strip().lower()
    if val == 'true': return "ACTIVE"
    if val == 'false': return "INACTIVE"
    return "N/A"

def create_property_dict(row):
    """Molde para la HOJA 1: Inventario de Propiedades (Solo 4 Columnas útiles)"""
    return {
        "COUNTRY": row.get('country', 'N/A'),
        "PROPERTY": row.get('property', 'N/A'),
        "OFFBOARDING GUESTY": format_date_mx(row.get('offboarding_date')),
        "STATUS GUESTY": get_status_guesty(row.get('status_json'))
    }

def create_reservation_dict(row, is_violation=True):
    """Molde para las HOJAS 2 y 3: Detalle de Reservas (8 Columnas completas)"""
    status_icon = "❌ ALERTA" if is_violation else "✅ OK"
    return {
        "COUNTRY": row.get('country', 'N/A'),
        "PROPERTY": row.get('property', 'N/A'),
        "CONFIRMATION CODE": row.get('confirmation_code', 'N/A'),
        "OFFBOARDING GUESTY": format_date_mx(row.get('offboarding_date')),
        "CHECK IN": format_date_mx(row.get('check_in_date')),
        "CHECK OUT": format_date_mx(row.get('check_out_date')),
        "STATUS": status_icon,
        "STATUS GUESTY": get_status_guesty(row.get('status_json'))
    }

def send_alert_email(proactive_data, alerts_data, reactive_data):
    if not proactive_data and not alerts_data and not reactive_data: 
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
    
    # Hoja 1: Solo inventario de propiedades (4 columnas)
    list_proactive = [create_property_dict(row) for row in proactive_data] if proactive_data else []
    
    # Hoja 2: Alertas activas (8 columnas)
    list_alerts = []
    current_alert_count = 0
    if alerts_data:
        for row in alerts_data:
            list_alerts.append(create_reservation_dict(row, is_violation=True))
            current_alert_count += 1

    # Hoja 3: Alertas históricas/reactivas (8 columnas)
    list_reactive = []
    reactive_count = 0
    if reactive_data:
        for row in reactive_data:
            list_reactive.append(create_reservation_dict(row, is_violation=True))
            reactive_count += 1

    # Definir los encabezados exactos para que no haya columnas en blanco/inútiles
    cols_properties = ["COUNTRY", "PROPERTY", "OFFBOARDING GUESTY", "STATUS GUESTY"]
    cols_reservations = ["COUNTRY", "PROPERTY", "CONFIRMATION CODE", "OFFBOARDING GUESTY", "CHECK IN", "CHECK OUT", "STATUS", "STATUS GUESTY"]
    
    df_proactive = pd.DataFrame(list_proactive, columns=cols_properties)
    df_alerts = pd.DataFrame(list_alerts, columns=cols_reservations)
    df_reactive = pd.DataFrame(list_reactive, columns=cols_reservations)

    # --- 2. CREAR EXCEL EN MEMORIA ---
    excel_buffer = io.BytesIO()
    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
        df_proactive.to_excel(writer, sheet_name='Inventario Deptos', index=False)
        df_alerts.to_excel(writer, sheet_name='Alertas Activas', index=False)
        df_reactive.to_excel(writer, sheet_name='Alertas Históricas', index=False)
    
    excel_data = excel_buffer.getvalue()

    # --- 3. CONFIGURAR EL CORREO ---
    msg = MIMEMultipart()
    total_alerts = current_alert_count + reactive_count
    msg['Subject'] = f"📊 Reporte de Alertas Offboarding: {current_alert_count} Activas / {reactive_count} Históricas"
    msg['From'] = sender
    msg['To'] = ", ".join(recipients)

    # Cuerpo del correo HTML más bonito y cálido
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
                    <li><b>🚨 Alertas Activas (Departamentos Activos):</b> {current_alert_count} reservas detectadas.</li>
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
    msg.attach(MIMEText(html_body, 'html'))

    # --- 4. ADJUNTAR EL EXCEL ---
    part = MIMEBase('application', 'vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    part.set_payload(excel_data)
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', 'attachment; filename="Alertas_Offboarding.xlsx"')
    msg.attach(part)

    # --- 5. ENVIAR CORREO ---
    try:
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.login(sender, password)
        server.sendmail(sender, recipients, msg.as_string())
        server.quit()
        logger.info("✅ Reporte Excel (Diseño Limpio) enviado con éxito.")
    except Exception as e:
        logger.error(f"❌ Falló envío: {e}")