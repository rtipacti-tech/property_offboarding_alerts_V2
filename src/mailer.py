import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

load_dotenv()

def send_alert_email(bookings):
    if not bookings:
        return

    sender = os.getenv("EMAIL_SENDER")
    password = os.getenv("EMAIL_PASSWORD")
    
    # 1. Leemos las variables del .env
    cx_emails = os.getenv("EMAIL_CX", "")
    rodrigo_emails = os.getenv("EMAIL_RODRIGO", "")

    # 2. Unimos todo en una sola cadena y luego separamos por comas
    all_emails_string = f"{cx_emails},{rodrigo_emails}"
    
    # 3. Limpiamos la lista (quitamos espacios y vacíos)
    recipients = [email.strip() for email in all_emails_string.split(",") if email.strip()]

    # Si no hay destinatarios válidos, salimos
    if not recipients:
        print("⚠️ Error: No hay destinatarios configurados en el .env")
        return

    msg = MIMEMultipart()
    msg['Subject'] = f"🚨 URGENTE: {len(bookings)} Reservas Detectadas en Propiedades OFFBOARDING"
    msg['From'] = sender
    msg['To'] = ", ".join(recipients) # Esto pone todos los correos en el encabezado visible

    # Construir filas de la tabla
    rows = ""
    for b in bookings:
        in_date = str(b['check_in'])
        out_date = str(b['check_out'])
        
        rows += f"""
        <tr>
            <td style="padding: 8px; border: 1px solid #ccc;"><b>{b['property_name']}</b></td>
            <td style="padding: 8px; border: 1px solid #ccc; color: red;">{b['property_status']}</td>
            <td style="padding: 8px; border: 1px solid #ccc;">{b['confirmation_code']}</td>
            <td style="padding: 8px; border: 1px solid #ccc;">{in_date}</td>
            <td style="padding: 8px; border: 1px solid #ccc;">{out_date}</td>
        </tr>
        """

    html = f"""
    <html>
    <body style="font-family: Arial, sans-serif;">
        <h2 style="color: #D32F2F;">⚠️ Acción Requerida: Offboarding con Reservas</h2>
        <p>El sistema ha detectado <b>{len(bookings)} reservas activas</b> en propiedades que figuran como <b>INACTIVAS</b>.</p>
        <p>Es necesario mover estas reservas inmediatamente:</p>
        
        <table style="width: 100%; border-collapse: collapse; text-align: left;">
            <tr style="background-color: #f2f2f2;">
                <th style="padding: 10px; border: 1px solid #ccc;">Propiedad</th>
                <th style="padding: 10px; border: 1px solid #ccc;">Estado</th>
                <th style="padding: 10px; border: 1px solid #ccc;">Cód. Reserva</th>
                <th style="padding: 10px; border: 1px solid #ccc;">Check-IN</th>
                <th style="padding: 10px; border: 1px solid #ccc;">Check-OUT</th>
            </tr>
            {rows}
        </table>
        <br>
        <p style="font-size: 12px; color: #777;">Monitor de Integridad - Ejecución Automática</p>
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
        print(f"✅ Alerta enviada correctamente a: {recipients}")
    except Exception as e:
        print(f"❌ Error enviando correo: {e}")