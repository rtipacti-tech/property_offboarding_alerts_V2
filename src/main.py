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
    logger.info("🚀 Iniciando Monitor de Offboarding (Reporte Visual)...")
    
    try:
        # 1. Obtener reporte completo (Reservas de propiedades en cierre)
        # Trae tanto las conflictivas como las sanas para mostrarlas en el correo.
        report_data = database.get_offboarding_report()
        
        # 2. Enviar correo siempre que haya datos
        # (El mailer se encargará de pintar rojo/verde según corresponda)
        if report_data:
            logger.info(f"📨 Enviando reporte con {len(report_data)} filas...")
            mailer.send_alert_email(report_data)
            logger.info("🏁 Proceso finalizado exitosamente.")
        else:
            logger.info("✅ No hay propiedades en proceso de cierre o sin reservas recientes.")

    except Exception as e:
        logger.critical(f"💀 Error fatal en el proceso principal: {e}", exc_info=True)

if __name__ == "__main__":
    run_job()