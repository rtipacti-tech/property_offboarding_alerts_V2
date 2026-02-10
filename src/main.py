import logging
import sys
from dotenv import load_dotenv
import database
import mailer

# --- CONFIGURACIÓN DE LOGS ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

load_dotenv()

def run_job():
    logger.info("🚀 Iniciando ejecución programada del Monitor de Offboarding...")
    
    try:
        # 1. Buscamos conflictos
        conflicts = database.find_orphaned_bookings()
        
        # 2. Decisión lógica
        if conflicts:
            logger.warning(f"⚠️ ALERTA: Se encontraron {len(conflicts)} problemas. Procediendo a notificar.")
            mailer.send_alert_email(conflicts)
            logger.info("🏁 Proceso finalizado con envío de correos.")
        else:
            logger.info("✅ Sin novedades. El sistema está limpio. No se envía correo.")

    except Exception as e:
        logger.critical(f"💀 Error fatal en el proceso principal: {e}", exc_info=True)

if __name__ == "__main__":
    run_job()