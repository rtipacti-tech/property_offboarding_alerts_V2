import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
import logging

# Configuración del logger para este archivo
logger = logging.getLogger(__name__)

load_dotenv()

def send_alert_email(bookings):
    """
    Envía un correo HTML con la tabla de conflictos detectados.
    Recibe una lista de diccionarios con claves: 
    'property_name', 'conflict_reason', 'confirmation_code', 'res_check_in', 'res_check_out'
    """
    # Si no hay reservas, no hacemos nada
    if not bookings:
        return

    sender = os.getenv("EMAIL_SENDER")
    password = os.getenv("EMAIL_PASSWORD")
    
    # 1. Lógica de destinatarios: CX + Rodrigo
    cx_emails = os.getenv("EMAIL_CX", "")
    rodrigo_emails = os.getenv("EMAIL_RODRIGO", "")
    
    # Unimos, separamos por comas y limpiamos espacios vacíos
    all_emails_string = f"{cx_emails},{rodrigo_emails}"
    recipients = [email.strip() for email in all_emails_string.split(",") if email.strip()]

    if not recipients:
        logger.error("⚠️ Error: No se encontraron destinatarios en el archivo .env (EMAIL_CX / EMAIL_RODRIGO)")
        return

    # 2. Crear el objeto del mensaje
    msg = MIMEMultipart()
    msg['Subject'] = f"🚨 ALERTA CRÍTICA: {len(bookings)} Reservas sobre Bloqueos OFFBOARDING"
    msg['From'] = sender
    msg['To'] = ", ".join(recipients)

    # 3. Construcción de la Tabla HTML
    rows = ""
    for b in bookings:
        # Usamos .get() para evitar errores si falta algún dato
        prop_name = b.get('property_name', 'Desconocida')
        conflict = b.get('conflict_reason', 'BLOOFF') # Si no viene, asumimos BLOOFF
        code = b.get('confirmation_code', 'N/A')
        check_in = str(b.get('res_check_in', 'N/A'))
        check_out = str(b.get('res_check_out', 'N/A'))
        
        rows += f"""
        <tr>
            <td style="padding: 8px; border: 1px solid #ddd;"><b>{prop_name}</b></td>
            <td style="padding: 8px; border: 1px solid #ddd; color: #D32F2F; font-weight: bold; text-align: center;">{conflict}</td>
            <td style="padding: 8px; border: 1px solid #ddd;">{code}</td>
            <td style="padding: 8px; border: 1px solid #ddd;">{check_in}</td>
            <td style="padding: 8px; border: 1px solid #ddd;">{check_out}</td>
        </tr>
        """

    # 4. Plantilla HTML Profesional
    html = f"""
    <html>
    <body style="font-family: Arial, sans-serif; color: #333;">
        <div style="max-width: 600px; margin: auto; border: 1px solid #eee; padding: 20px; border-radius: 8px;">
            <h2 style="color: #D32F2F; border-bottom: 2px solid #D32F2F; padding-bottom: 10px;">
                ⚠️ Acción Requerida: Conflicto de Calendario
            </h2>
            
            <p>El sistema de monitoreo ha detectado <b>{len(bookings)} reservas confirmadas</b> que se solapan con un bloqueo de <b>OFFBOARDING (BLOOFF)</b>.</p>
            
            <p style="background-color: #fff3cd; padding: 10px; border-left: 4px solid #ffc107; font-size: 13px;">
                <b>Lógica de Detección:</b> Estas reservas ocurren en fechas marcadas como 'BLOQUEO POR SALIDA' en el calendario maestro (Block Gold).
            </p>

            <table style="width: 100%; border-collapse: collapse; font-size: 13px; margin-top: 20px;">
                <thead>
                    <tr style="background-color: #f8f9fa; text-align: left;">
                        <th style="padding: 10px; border: 1px solid #ddd;">Propiedad</th>
                        <th style="padding: 10px; border: 1px solid #ddd;">Motivo</th>
                        <th style="padding: 10px; border: 1px solid #ddd;">Reserva</th>
                        <th style="padding: 10px; border: 1px solid #ddd;">Check-IN</th>
                        <th style="padding: 10px; border: 1px solid #ddd;">Check-OUT</th>
                    </tr>
                </thead>
                <tbody>
                    {rows}
                </tbody>
            </table>
            
            <br>
            <p>Por favor, gestionar la reubicación inmediatamente.</p>
            <hr style="border: 0; border-top: 1px solid #eee;">
            <p style="font-size: 11px; color: #999; text-align: center;">
                Generado autom. por Property Offboarding Monitor v2.0
            </p>
        </div>
    </body>
    </html>
    """
    
    msg.attach(MIMEText(html, 'html'))

    # 5. Envío SMTP
    try:
        logger.info(f"📤 Conectando a Gmail SMTP para enviar a {len(recipients)} destinatarios...")
        
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender, password)
        server.sendmail(sender, recipients, msg.as_string())
        server.quit()
        
        logger.info("✅ Correo de alerta enviado exitosamente.")
        
    except Exception as e:
        logger.error(f"❌ Falló el envío del correo: {e}", exc_info=True)