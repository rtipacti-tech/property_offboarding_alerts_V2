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
    logger.info("🚀 Iniciando ejecución programada del Monitor de Offboarding (Modo Dual)...")
    
    try:
        # 1. Buscamos conflictos de Reservas (Listado Urgente)
        # (Reservas chocando con Bloqueos Físicos o Fecha Oficial)
        conflicts = database.find_orphaned_bookings()
        
        # 2. Buscamos el Listado Oficial de Offboarding (Listado Informativo)
        # (Propiedades con fecha de corte en 'offboarding_guesty' reciente o futura)
        official_list = database.find_offboarding_listings()
        
        # 3. Decisión lógica: Si hay datos en CUALQUIERA de las dos listas, enviamos el reporte.
        if conflicts or official_list:
            count_conflicts = len(conflicts)
            count_official = len(official_list)
            
            logger.warning(f"⚠️ REPORTE GENERADO: {count_conflicts} conflictos críticos y {count_official} propiedades en cierre.")
            logger.info("📧 Procediendo a enviar el correo combinado...")
            
            # Pasamos AMBAS listas al mailer
            mailer.send_alert_email(conflicts, official_list)
            
            logger.info("🏁 Proceso finalizado exitosamente.")
        else:
            logger.info("✅ Sin novedades. El sistema está limpio y no hay cierres recientes. No se envía correo.")

    except Exception as e:
        logger.critical(f"💀 Error fatal en el proceso principal: {e}", exc_info=True)

if __name__ == "__main__":
    run_job()