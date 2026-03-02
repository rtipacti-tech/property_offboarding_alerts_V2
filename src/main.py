"""
Main Entry Point Module.

Orchestrates the data retrieval from the database and the email reporting process.
Configures logging and handles high-level execution flow.
"""
import logging
from dotenv import load_dotenv
import database
import mailer

# Basic logging configuration to output to both console and a file.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("execution.log"),  # Saves logs to this file
        logging.StreamHandler()                # Shows logs in the console
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables (.env)
load_dotenv()

def run_job():
    """
    Executes the main workflow of the alert system.

    Steps:
    1. Retrieves the Proactive Inventory report.
    2. Retrieves Active Alerts (critical reservations).
    3. Retrieves Reactive History.
    4. If data exists in any report, it triggers the email sender.
    """
    logger.info("🚀 Starting Offboarding Strict Audit...")
    
    try:
        # 1. Get Sheet 1: Property Inventory (Active and > 30 days)
        logger.info("🔍 Querying Proactive Inventory...")
        proactive_list = database.get_proactive_report()
        
        # 2. Get Sheet 2: Urgent Alerts (Reservations in Active Depts)
        logger.info("🚨 Searching for Active Alerts...")
        alerts_list = database.get_active_alerts_report()
        
        # 3. Get Sheet 3: Historical Record (Reservations in Inactive Depts)
        logger.info("🗄️ Retrieving Reactive History...")
        reactive_list = database.get_reactive_report()
        
        # 4. Send data to Mailer to generate Excel and send email
        if proactive_list or alerts_list or reactive_list:
            logger.info(f"📨 Generating Excel: {len(proactive_list)} Properties / {len(alerts_list)} Alerts / {len(reactive_list)} Historical.")
            mailer.send_alert_email(proactive_list, alerts_list, reactive_list)
        else:
            logger.info("✅ System clean. No properties or alerts to report.")

    except Exception as e:
        logger.critical(f"💀 Fatal error in main execution: {e}", exc_info=True)

if __name__ == "__main__":
    run_job()