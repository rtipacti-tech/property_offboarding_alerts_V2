import logging
from dotenv import load_dotenv
import database
import mailer

# Configuración básica de los logs para ver en consola qué está pasando
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# Cargar variables de entorno (.env)
load_dotenv()

def run_job():
    logger.info("🚀 Iniciando Auditoría Estricta de Offboarding...")
    
    try:
        # 1. Obtener la Hoja 1: Inventario de Propiedades (Active y > 30 días)
        logger.info("🔍 Consultando Inventario Proactivo...")
        proactive_list = database.get_proactive_report()
        
        # 2. Obtener la Hoja 2: Alertas Urgentes (Reservas en Deptos Active)
        logger.info("🚨 Buscando Alertas Activas...")
        alerts_list = database.get_active_alerts_report()
        
        # 3. Obtener la Hoja 3: Registro Histórico (Reservas en Deptos Inactive)
        logger.info("🗄️ Recuperando Historial Reactivo...")
        reactive_list = database.get_reactive_report()
        
        # 4. Enviar los datos al Mailer para generar el Excel y enviar el correo
        if proactive_list or alerts_list or reactive_list:
            logger.info(f"📨 Generando Excel: {len(proactive_list)} Propiedades / {len(alerts_list)} Alertas / {len(reactive_list)} Históricas.")
            mailer.send_alert_email(proactive_list, alerts_list, reactive_list)
        else:
            logger.info("✅ Sistema inmaculado. No hay propiedades ni alertas para reportar.")

    except Exception as e:
        logger.critical(f"💀 Error fatal en la ejecución principal: {e}", exc_info=True)

if __name__ == "__main__":
    run_job()