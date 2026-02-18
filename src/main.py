import logging
from dotenv import load_dotenv
import database
import mailer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)
load_dotenv()

def run_job():
    logger.info("🚀 Iniciando Auditoría Offboarding...")
    
    try:
        # 1. Obtener Datos Proactivos (Recientes, Buenos y Malos)
        proactive_list = database.get_proactive_report()
        
        # 2. Obtener Datos Reactivos (Historial completo de Fallas)
        reactive_list = database.get_reactive_report()
        
        # 3. Enviar
        if proactive_list or reactive_list:
            logger.info(f"📨 Generando reporte: {len(proactive_list)} filas proactivas / {len(reactive_list)} históricas.")
            mailer.send_alert_email(proactive_list, reactive_list)
        else:
            logger.info("✅ Base de datos inmaculada. Nada que reportar.")

    except Exception as e:
        logger.critical(f"💀 Error fatal: {e}", exc_info=True)

if __name__ == "__main__":
    run_job()