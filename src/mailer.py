import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)
load_dotenv()

def send_alert_email(bookings):
    if not bookings:
        return

    sender = os.getenv("EMAIL_SENDER")
    password = os.getenv("EMAIL_PASSWORD")
    
    cx_emails = os.getenv("EMAIL_CX", "")
    rodrigo_emails = os.getenv("EMAIL_RODRIGO", "")
    all_emails_string = f"{cx_emails},{rodrigo_emails}"
    
    recipients = [email.strip() for email in all_emails_string.split(",") if email.strip()]

    if not recipients:
        logger.error("⚠️ Error: No hay destinatarios configurados en el .env")
        return

    msg = MIMEMultipart()
    msg['Subject'] = f"🚨 URGENTE: {len(bookings)} Reservas en Propiedades OFFBOARDING"
    msg['From'] = sender
    msg['To'] = ", ".join(recipients)

    # Construir filas (Tu lógica HTML estaba perfecta, la mantengo igual)
    rows = ""
    for b in bookings:
        rows += f"""
        <tr>
            <td style="padding: 8px; border: 1px solid #ccc;"><b>{b['property_name']}</b></td>
            <td style="padding: 8px; border: 1px solid #ccc; color: red;">{b['property_status']}</td>
            <td style="padding: 8px; border: 1px solid #ccc;">{b['confirmation_code']}</td>
            <td style="padding: 8px; border: 1px solid #ccc;">{str(b['check_in'])}</td>
            <td style="padding: 8px; border: 1px solid #ccc;">{str(b['check_out'])}</td>
        </tr>
        """

    html = f"""
    <html>
    <body style="font-family: Arial, sans-serif;">
        <h2 style="color: #D32F2F;">⚠️ Acción Requerida: Offboarding con Reservas</h2>
        <p>El sistema ha detectado <b>{len(bookings)} reservas activas</b> en propiedades INACTIVAS.</p>
        <table style="width: 100%; border-collapse: collapse; text-align: left;">
            <tr style="background-color: #f2f2f2;">
                <th>Propiedad</th><th>Estado</th><th>Cód. Reserva</th><th>Check-IN</th><th>Check-OUT</th>
            </tr>
            {rows}
        </table>
        <br><p style="font-size: 12px; color: #777;">Monitor Automático</p>
    </body>
    </html>
    """
    
    msg.attach(MIMEText(html, 'html'))

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender, password)
        server.sendmail(sender, recipients, msg.as_string())
        server.quit()
        logger.info(f"✅ Correo de alerta enviado exitosamente a: {len(recipients)} destinatarios.")
    except Exception as e:
        logger.error(f"❌ Fallo al enviar correo SMTP: {e}", exc_info=True)