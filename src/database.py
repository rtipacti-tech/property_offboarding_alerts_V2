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
            password=os.getenv("DB_PASS"),
            port=os.getenv("DB_PORT")
        )
        return conn
    except Exception as e:
        logger.error(f"❌ Error de conexión a BD: {e}")
        return None

def get_offboarding_report():
    """
    Trae reservas de propiedades con fecha de corte (reciente o futura).
    IMPORTANTE: Trae el Check-Out convertido a objeto fecha para validar si se pasan del límite.
    """
    conn = get_db_connection()
    if not conn: return []

    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
        SELECT 
            r."LISTING'S NICKNAME" AS "property",
            r."CONFIRMATION CODE" AS "confirmation_code",
            mv.offboarding_guesty AS "offboarding_date",
            
            -- CHECK-IN (Fecha Real)
            TO_DATE(regexp_replace(r."CHECK IN", '[[:space:]]+', '', 'g'), 'DD/MM/YYYY') AS "check_in_date",
            
            -- CHECK-OUT (Fecha Real)
            TO_DATE(regexp_replace(r."CHECK OUT", '[[:space:]]+', '', 'g'), 'DD/MM/YYYY') AS "check_out_date",
            
            -- Textos originales para mostrar en tabla
            r."CHECK IN" AS "check_in_str",
            r."CHECK OUT" AS "check_out_str"
            
        FROM reservation_gold r
        JOIN mv_listings mv ON r."LISTING'S NICKNAME" = mv.nickname
        WHERE 
            lower(trim(r."STATUS")) = 'confirmed'
            
            -- Filtro: Propiedades con fecha de corte reciente o futura
            AND mv.offboarding_guesty IS NOT NULL
            AND mv.offboarding_guesty >= (CURRENT_DATE - INTERVAL '30 days')
            
            -- Filtro: Reservas vivas (que terminan hoy o después, o hace poco)
            AND TO_DATE(regexp_replace(r."CHECK OUT", '[[:space:]]+', '', 'g'), 'DD/MM/YYYY') >= (CURRENT_DATE - INTERVAL '30 days')

        ORDER BY mv.offboarding_guesty ASC, "check_in_date" ASC;
        """
        
        cur.execute(query)
        results = cur.fetchall()
        
        logger.info(f"📊 Datos obtenidos: {len(results)} reservas.")
        
        cur.close()
        conn.close()
        return results

    except Exception as e:
        logger.error(f"❌ Error en Query: {e}", exc_info=True)
        if conn: conn.close()
        return []