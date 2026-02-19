import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)
load_dotenv()

def format_date_mx(date_obj):
    if date_obj: return date_obj.strftime('%d/%m/%Y')
    return "N/A"

def create_row_html(row, is_violation=False):
    """Ayuda a crear filas HTML para no repetir código"""
    country = row.get('country', 'N/A')
    prop = row.get('property', 'N/A')
    code = row.get('confirmation_code', 'N/A')
    
    off_date_str = format_date_mx(row.get('offboarding_date'))
    check_in_str = format_date_mx(row.get('check_in_date'))
    check_out_str = format_date_mx(row.get('check_out_date'))
    
    # --- TRADUCCIÓN DE STATUS GUESTY (True/False -> Active/Inactive) ---
    raw_guesty = str(row.get('status_json', '')).strip().lower()
    
    if raw_guesty == 'true':
        status_guesty = "ACTIVE"
    elif raw_guesty == 'false':
        status_guesty = "INACTIVE"
    else:
        status_guesty = "N/A"
    # -------------------------------------------------------------------

    if is_violation:
        status_icon = "❌ ALERTA"
        row_style = "background-color: #ffebee; color: #c62828; font-weight: bold;"
    else:
        status_icon = "✅ OK"
        row_style = "color: #2e7d32;"

    return f"""
    <tr style="{row_style}">
        <td style="padding: 8px; border: 1px solid #ddd;">{country}</td>
        <td style="padding: 8px; border: 1px solid #ddd;">{prop}</td>
        <td style="padding: 8px; border: 1px solid #ddd;">{code}</td>
        <td style="padding: 8px; border: 1px solid #ddd;">{off_date_str}</td>
        <td style="padding: 8px; border: 1px solid #ddd;">{check_in_str}</td>
        <td style="padding: 8px; border: 1px solid #ddd;">{check_out_str}</td>
        <td style="padding: 8px; border: 1px solid #ddd; text-align: center;">{status_icon}</td>
        <td style="padding: 8px; border: 1px solid #ddd; text-align: center;">{status_guesty}</td>
    </tr>
    """

def send_alert_email(proactive_data, reactive_data):
    if not proactive_data and not reactive_data: return

    sender = os.getenv("EMAIL_SENDER")
    password = os.getenv("EMAIL_PASSWORD")
    cx_emails = os.getenv("EMAIL_CX", "")
    rodrigo_emails = os.getenv("EMAIL_RODRIGO", "")
    all_emails_string = f"{cx_emails},{rodrigo_emails}"
    recipients = [email.strip() for email in all_emails_string.split(",") if email.strip()]

    if not recipients: return

    msg = MIMEMultipart()
    total_alerts = 0
    
    # 1. PROCESAR SISTEMA PROACTIVO Y ALERTAS ACTUALES
    rows_proactive = ""
    rows_current_alerts = ""
    current_alert_count = 0

    if proactive_data:
        for row in proactive_data:
            # Detectar violación
            off = row.get('offboarding_date')
            out = row.get('check_out_date')
            is_bad = (out and off and out > off)

            html_row = create_row_html(row, is_bad)
            rows_proactive += html_row
            
            if is_bad:
                rows_current_alerts += html_row
                current_alert_count += 1

    # 2. PROCESAR SISTEMA REACTIVO (HISTÓRICO)
    rows_reactive = ""
    reactive_count = 0
    if reactive_data:
        for row in reactive_data:
            # Aquí TODAS son malas por definición de la query
            rows_reactive += create_row_html(row, is_violation=True)
            reactive_count += 1

    total_alerts = current_alert_count + reactive_count
    msg['Subject'] = f"📊 Reporte Offboarding: {current_alert_count} Alertas Activas / {reactive_count} Históricas"
    msg['From'] = sender
    msg['To'] =", ".join(recipients)

    # --- HTML ---
    # Tablas vacías si no hay datos. Actualizado a colspan="8" por la nueva columna.
    empty_row = '<tr><td colspan="8" style="padding:15px; text-align:center; color:#666; background:#f9f9f9;">✅ Sin datos para mostrar.</td></tr>'
    
    table_proactive = rows_proactive if rows_proactive else empty_row
    table_alerts = rows_current_alerts if rows_current_alerts else empty_row
    table_reactive = rows_reactive if rows_reactive else empty_row

    html = f"""
    <html>
    <body style="font-family: Arial, sans-serif; color: #333; font-size: 12px;">
        <div style="max-width: 1100px; margin: auto; padding: 20px; border: 1px solid #ccc;">
            <h2 style="border-bottom: 2px solid #333;">📅 Monitor Global de Offboarding</h2>

            <h3 style="color: #1565C0; background: #E3F2FD; padding: 5px;">🔹 Sistema Proactivo (Panorama Actual -30 días).</h3>
            <table style="width: 100%; border-collapse: collapse;">
                <tr style="background:#f0f0f0; text-align:left;">
                    <th>COUNTRY</th><th>PROPERTY</th><th>CONFIRMATION CODE</th><th>OFFBOARDING GUESTY</th><th>CHECK IN</th><th>CHECK OUT</th><th style="text-align: center;">STATUS</th><th style="text-align: center;">STATUS GUESTY</th>
                </tr>
                {table_proactive}
            </table>
            <br>

            <h3 style="color: #c62828; background: #FFEBEE; padding: 5px;">🚨 Alertas Activas (Requieren Acción)</h3>
            <table style="width: 100%; border-collapse: collapse;">
                <tr style="background:#ffebee; color:#c62828; text-align:left;">
                    <th>COUNTRY</th><th>PROPERTY</th><th>CONFIRMATION CODE</th><th>OFFBOARDING GUESTY</th><th>CHECK IN</th><th>CHECK OUT</th><th style="text-align: center;">STATUS</th><th style="text-align: center;">STATUS GUESTY</th>
                </tr>
                {table_alerts}
            </table>
            <br>

            <h3 style="color: #424242; background: #EEEEEE; padding: 5px;">⚫ Sistema Reactivo (Historial de Incidencias)</h3>
            <p>Todas las reservas en la historia de la BD que terminaron después de la fecha de offboarding.</p>
            <table style="width: 100%; border-collapse: collapse;">
                <tr style="background:#616161; color:white; text-align:left;">
                    <th>COUNTRY</th><th>PROPERTY</th><th>CONFIRMATION CODE</th><th>OFFBOARDING GUESTY</th><th>CHECK IN</th><th>CHECK OUT</th><th style="text-align: center;">STATUS</th><th style="text-align: center;">STATUS GUESTY</th>
                </tr>
                {table_reactive}
            </table>

        </div>
    </body>
    </html>
    """
    msg.attach(MIMEText(html, 'html'))

    try:
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.login(sender, password)
        server.sendmail(sender, recipients, msg.as_string())
        server.quit()
        logger.info("✅ Reporte Completo (3 Secciones + Guesty) enviado.")
    except Exception as e:
        logger.error(f"❌ Falló envío: {e}")