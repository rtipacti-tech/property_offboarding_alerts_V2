import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)
load_dotenv()

def send_alert_email(data):
    if not data: return

    sender = os.getenv("EMAIL_SENDER")
    password = os.getenv("EMAIL_PASSWORD")
    
    # Destinatarios
    cx_emails = os.getenv("EMAIL_CX", "")
    rodrigo_emails = os.getenv("EMAIL_RODRIGO", "")
    all_emails_string = f"{cx_emails},{rodrigo_emails}"
    recipients = [email.strip() for email in all_emails_string.split(",") if email.strip()]

    if not recipients:
        logger.error("⚠️ Error: No hay destinatarios.")
        return

    msg = MIMEMultipart()
    msg['Subject'] = f"📊 Reporte Offboarding: {len(data)} Reservas (Validación por Check-Out)"
    msg['From'] = sender
    msg['To'] = sender 

    rows = ""
    alert_count = 0

    for row in data:
        prop = row.get('property', 'N/A')
        code = row.get('confirmation_code', 'N/A')
        
        # Objetos de fecha reales
        off_date = row.get('offboarding_date') 
        check_out_date = row.get('check_out_date')
        
        # Textos para mostrar
        off_date_str = str(off_date)
        check_in_str = row.get('check_in_str', 'N/A')
        check_out_str = row.get('check_out_str', 'N/A')

        # --- LÓGICA DE ALERTA CORREGIDA ---
        # Si la reserva TERMINA después de la fecha de Offboarding -> ALERTA
        # (Ej: Cierra el 10, sale el 11 -> MAL)
        if check_out_date and off_date and check_out_date > off_date:
            status_icon = "❌ ALERTA"
            # Rojo claro para destacar el error
            row_style = "background-color: #ffebee; color: #c62828; font-weight: bold;"
            alert_count += 1
        else:
            status_icon = "✅ OK"
            # Verde normal
            row_style = "color: #2e7d32;"

        rows += f"""
        <tr style="{row_style}">
            <td style="padding: 10px; border: 1px solid #ddd;">{prop}</td>
            <td style="padding: 10px; border: 1px solid #ddd;">{code}</td>
            <td style="padding: 10px; border: 1px solid #ddd;">{off_date_str}</td>
            <td style="padding: 10px; border: 1px solid #ddd;">{check_in_str}</td>
            <td style="padding: 10px; border: 1px solid #ddd;">{check_out_str}</td>
            <td style="padding: 10px; border: 1px solid #ddd; text-align: center; font-size: 14px;">{status_icon}</td>
        </tr>
        """

    html = f"""
    <html>
    <body style="font-family: Arial, sans-serif; color: #333;">
        <div style="max-width: 900px; margin: auto; padding: 20px; border: 1px solid #eee; border-radius: 8px;">
            <h2 style="color: #333; border-bottom: 2px solid #333; padding-bottom: 10px;">
                📅 Monitor de Offboarding
            </h2>
            <p>Validación: Reservas que finalizan después de la fecha de offboarding_guesty.</p>
            
            <div style="margin-bottom: 20px; padding: 10px; background-color: #f5f5f5; border-radius: 5px;">
                <strong>Resumen:</strong> <span style="color: #c62828; font-weight: bold;">{alert_count} Conflictos</span> detectados de {len(data)} reservas analizadas.
            </div>

            <table style="width: 100%; border-collapse: collapse; font-size: 12px;">
                <thead>
                    <tr style="background-color: #333; color: white; text-align: left;">
                        <th style="padding: 10px;">PROPIEDAD</th>
                        <th style="padding: 10px;">CODIGO DE CONFIRMACION</th>
                        <th style="padding: 10px;">FECHA DE OFF_GUESTY</th>
                        <th style="padding: 10px;">CHECK IN</th>
                        <th style="padding: 10px;">CHECK OUT</th>
                        <th style="padding: 10px; text-align: center;">ESTADO</th>
                    </tr>
                </thead>
                <tbody>
                    {rows}
                </tbody>
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
        logger.info("✅ Reporte enviado.")
    except Exception as e:
        logger.error(f"❌ Falló envío: {e}")