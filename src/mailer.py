import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)
load_dotenv()

def send_alert_email(booking_conflicts, offboarding_listings):
    """
    Envía un correo con DOS tablas:
    1. Conflictos de Reservas (Urgente).
    2. Lista Oficial de Propiedades en Offboarding (Informativo).
    """
    # Si no hay nada en ninguna lista, no enviamos nada
    if not booking_conflicts and not offboarding_listings:
        return

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
    msg['Subject'] = f"📊 Reporte Offboarding: {len(booking_conflicts)} Conflictos / {len(offboarding_listings)} Propiedades Cerrando"
    msg['From'] = sender
    msg['To'] = sender  # BCC visual

    # --- TABLA 1: CONFLICTOS DE RESERVA ---
    rows_conflicts = ""
    if booking_conflicts:
        for b in booking_conflicts:
            prop_name = b.get('property_name', 'N/A')
            reason = b.get('conflict_reason', 'N/A')
            code = b.get('confirmation_code', 'N/A')
            check_in = str(b.get('res_check_in', 'N/A'))
            check_out = str(b.get('res_check_out', 'N/A'))
            
            # Color rojo si es Activo, Gris si es Histórico
            color = "#D32F2F" if "ACTIVO" in reason else "#757575"
            
            rows_conflicts += f"""
            <tr>
                <td style="padding: 8px; border: 1px solid #ddd;"><b>{prop_name}</b></td>
                <td style="padding: 8px; border: 1px solid #ddd; color: {color}; font-weight: bold;">{reason}</td>
                <td style="padding: 8px; border: 1px solid #ddd;">{code}</td>
                <td style="padding: 8px; border: 1px solid #ddd;">{check_in}</td>
                <td style="padding: 8px; border: 1px solid #ddd;">{check_out}</td>
            </tr>
            """
        table_conflicts = f"""
        <h3 style="color: #D32F2F;">🚨 1. Conflictos de Reservas Detectados</h3>
        <table style="width: 100%; border-collapse: collapse; font-size: 12px;">
            <tr style="background-color: #f8f9fa;"><th>Propiedad</th><th>Motivo</th><th>Reserva</th><th>Check-IN</th><th>Check-OUT</th></tr>
            {rows_conflicts}
        </table>
        """
    else:
        table_conflicts = "<p>✅ No hay conflictos de reservas detectados.</p>"

    # --- TABLA 2: LISTA OFICIAL (FECHAS DE CORTE) ---
    rows_off = ""
    if offboarding_listings:
        for l in offboarding_listings:
            prop = l.get('property_name', 'N/A')
            date_off = str(l.get('offboarding_date', 'N/A'))
            country = l.get('country', 'N/A')
            
            rows_off += f"""
            <tr>
                <td style="padding: 8px; border: 1px solid #ddd;"><b>{prop}</b></td>
                <td style="padding: 8px; border: 1px solid #ddd;">{date_off}</td>
                <td style="padding: 8px; border: 1px solid #ddd;">{country}</td>
            </tr>
            """
        table_off = f"""
        <h3 style="color: #1976D2; margin-top: 30px;">📋 2. Propiedades con Fecha de Corte (Últimos 30 días y a Futuro)</h3>
        <p style="font-size: 11px; color: #666;">Fuente: Columna 'offboarding_guesty' en Power BI</p>
        <table style="width: 100%; border-collapse: collapse; font-size: 12px;">
            <tr style="background-color: #e3f2fd;"><th>Propiedad</th><th>Fecha Corte</th><th>País</th></tr>
            {rows_off}
        </table>
        """
    else:
        table_off = "<p>ℹ️ No hay propiedades con fecha de corte reciente.</p>"

    # HTML FINAL
    html = f"""
    <html>
    <body style="font-family: Arial, sans-serif;">
        <div style="max-width: 700px; margin: auto; padding: 20px; border: 1px solid #eee;">
            {table_conflicts}
            <hr style="border:0; border-top:1px solid #eee; margin: 20px 0;">
            {table_off}
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
        logger.info("✅ Reporte combinado enviado exitosamente.")
    except Exception as e:
        logger.error(f"❌ Falló envío: {e}")