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

# --- CONSULTA 1: SISTEMA PROACTIVO (Reciente y Futuro) ---
def get_proactive_report():
    conn = get_db_connection()
    if not conn: return []
    cur = None
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        query = """
        SELECT 
            mv.country AS "country",
            r."LISTING'S NICKNAME" AS "property",
            r."CONFIRMATION CODE" AS "confirmation_code",
            mv.offboarding_guesty AS "offboarding_date",
            
            -- EXTRAEMOS EL JSON ('active')
            gl.data ->> 'active' AS "status_json",
            
            TO_DATE(regexp_replace(r."CHECK IN", '[[:space:]]+', '', 'g'), 'DD/MM/YYYY') AS "check_in_date",
            TO_DATE(regexp_replace(r."CHECK OUT", '[[:space:]]+', '', 'g'), 'DD/MM/YYYY') AS "check_out_date"
            
        FROM reservation_gold r
        JOIN mv_listings mv ON r."LISTING'S NICKNAME" = mv.nickname
        
        -- UNIMOS LA TABLA DEL JSON
        LEFT JOIN guesty_listing gl ON r."LISTING'S NICKNAME" = gl.nickname
        
        WHERE 
            lower(trim(r."STATUS")) = 'confirmed'
            AND mv.offboarding_guesty IS NOT NULL
            -- Filtro de tiempo: Últimos 30 días o Futuro
            AND mv.offboarding_guesty >= (CURRENT_DATE - INTERVAL '30 days')
            AND TO_DATE(regexp_replace(r."CHECK OUT", '[[:space:]]+', '', 'g'), 'DD/MM/YYYY') >= (CURRENT_DATE - INTERVAL '30 days')
            
        ORDER BY mv.offboarding_guesty ASC, "check_in_date" ASC;
        """
        cur.execute(query)
        results = cur.fetchall()
        return results
    except Exception as e:
        logger.error(f"❌ Error Proactivo: {e}")
        return []
    finally:
        if cur: cur.close()
        if conn: conn.close()

# --- CONSULTA 2: SISTEMA REACTIVO (Histórico Completo) ---
def get_reactive_report():
    """
    Busca TODAS las reservas en la historia donde el Check-out fue posterior al Offboarding.
    Sin límite de fechas.
    """
    conn = get_db_connection()
    if not conn: return []
    cur = None
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        query = """
        SELECT 
            mv.country AS "country",
            r."LISTING'S NICKNAME" AS "property",
            r."CONFIRMATION CODE" AS "confirmation_code",
            mv.offboarding_guesty AS "offboarding_date",
            
            -- EXTRAEMOS EL JSON ('active')
            gl.data ->> 'active' AS "status_json",
            
            TO_DATE(regexp_replace(r."CHECK IN", '[[:space:]]+', '', 'g'), 'DD/MM/YYYY') AS "check_in_date",
            TO_DATE(regexp_replace(r."CHECK OUT", '[[:space:]]+', '', 'g'), 'DD/MM/YYYY') AS "check_out_date"
            
        FROM reservation_gold r
        JOIN mv_listings mv ON r."LISTING'S NICKNAME" = mv.nickname
        
        -- UNIMOS LA TABLA DEL JSON
        LEFT JOIN guesty_listing gl ON r."LISTING'S NICKNAME" = gl.nickname
        
        WHERE 
            lower(trim(r."STATUS")) = 'confirmed'
            AND mv.offboarding_guesty IS NOT NULL
            
            -- LA CONDICIÓN ÚNICA: VIOLACIÓN DE FECHA
            -- (Check Out real es estrictamente mayor que la fecha de corte)
            AND TO_DATE(regexp_replace(r."CHECK OUT", '[[:space:]]+', '', 'g'), 'DD/MM/YYYY') > mv.offboarding_guesty
            
        ORDER BY "check_out_date" DESC; -- Ordenamos por las más recientes violaciones primero
        """
        cur.execute(query)
        results = cur.fetchall()
        return results
    except Exception as e:
        logger.error(f"❌ Error Reactivo: {e}")
        return []
    finally:
        if cur: cur.close()
        if conn: conn.close()