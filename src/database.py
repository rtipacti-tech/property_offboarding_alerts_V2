import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)
load_dotenv()

def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"), # Asegúrate que en tu .env se llame DB_PASS
            port=os.getenv("DB_PORT")
        )
        return conn
    except Exception as e:
        logger.error(f"❌ Error de conexión a BD: {e}")
        return None

def find_orphaned_bookings():
    conn = get_db_connection()
    if not conn: 
        logger.warning("⚠️ Saltando consulta por fallo de conexión.")
        return []

    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
            SELECT 
                l.nickname AS property_name, 
                l.status AS property_status,
                r.confirmation_code,
                r.status AS reservation_status,
                r.check_in,
                r.check_out
            FROM guesty_listing l
            JOIN guesty_reservation r ON l.id = r.listing_id
            WHERE 
                l.status = 'inactive'
                AND r.status IN ('confirmed', 'reserved')
                AND r.check_in >= CURRENT_DATE
            ORDER BY r.check_in ASC;
        """
        
        cur.execute(query)
        results = cur.fetchall()
        
        logger.info(f"🔍 Consulta SQL ejecutada. Filas obtenidas: {len(results)}")
        
        cur.close()
        conn.close()
        return results

    except Exception as e:
        logger.error(f"❌ Error SQL ejecutando query: {e}", exc_info=True)
        if conn: conn.close()
        return []