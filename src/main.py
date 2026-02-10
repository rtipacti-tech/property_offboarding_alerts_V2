from dotenv import load_dotenv
from datetime import datetime
import database
import mailer

load_dotenv()

def run_job():
    print(f"--- 🕒 Ejecución programada: {datetime.now()} ---")
    
    # 1. Buscamos conflictos
    conflicts = database.find_orphaned_bookings()
    
    # 2. Decisión lógica
    if conflicts:
        print(f"⚠️ Se encontraron {len(conflicts)} problemas. ENVIANDO ALERTA.")
        mailer.send_alert_email(conflicts)
    else:
        # Aquí cae el caso de "Offboarding sin reservas" o "Propiedad Activa"
        print("✅ Sin novedades. No se envía correo.")

if __name__ == "__main__":
    run_job()