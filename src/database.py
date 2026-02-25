import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import logging
import time

logger = logging.getLogger(__name__)
load_dotenv()

def get_db_connection():
    max_intentos = 3
    for intento in range(max_intentos):
        try:
            conn = psycopg2.connect(
                host=os.getenv("DB_HOST"),
                database=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASS"),
                port=os.getenv("DB_PORT"),
                connect_timeout=10
            )
            return conn
        except (Exception, KeyboardInterrupt) as e:
            logger.warning(f"⚠️ Intento {intento + 1} fallido al conectar a la BD: {e}")
            if intento < max_intentos - 1:
                logger.info("🔄 Reintentando en 3 segundos...")
                time.sleep(3)
            else:
                logger.error("❌ Fallo definitivo al conectar a la BD.")
                return None

# ==========================================
# 1️⃣ HOJA 1: SISTEMA PROACTIVO (Inventario de Deptos Activos y > 30 días)
# ==========================================
def get_proactive_report():
    """
    Evalúa SOLO propiedades. Ignora las reservas.
    Filtro: Tienen fecha offboarding, siguen ACTIVE, y el blockoff fue hace más de 30 días.
    """
    conn = get_db_connection()
    if not conn: return []
    cur = None
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        query = """
        SELECT DISTINCT
            mv.country AS "country",
            mv.nickname AS "property",
            mv.offboarding_guesty AS "offboarding_date",
            gl.data ->> 'active' AS "status_json"
        FROM mv_listings mv
        -- 👇 INNER JOIN: Solo propiedades que realmente existen en Guesty
        JOIN guesty_listing gl ON mv.nickname = gl.nickname
        WHERE 
            mv.offboarding_guesty IS NOT NULL
            AND gl.data ->> '_id' IS NOT NULL
            
            -- CONDICIÓN 1: Siguen ACTIVE en Guesty
            AND lower(trim(gl.data ->> 'active')) = 'true'
            
            -- CONDICIÓN 2: Blockoff ocurrió hace más de 30 días
            AND mv.offboarding_guesty < (CURRENT_DATE - INTERVAL '30 days')
            
        ORDER BY mv.offboarding_guesty ASC;
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

# ==========================================
# 2️⃣ HOJA 2: ALERTAS ACTIVAS (Reservas en Deptos Activos con Violación)
# ==========================================
def get_active_alerts_report():
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
            gl.data ->> 'active' AS "status_json",
            TO_DATE(regexp_replace(r."CHECK IN", '[[:space:]]+', '', 'g'), 'DD/MM/YYYY') AS "check_in_date",
            TO_DATE(regexp_replace(r."CHECK OUT", '[[:space:]]+', '', 'g'), 'DD/MM/YYYY') AS "check_out_date"
        FROM reservation_gold r
        JOIN mv_listings mv ON r."LISTING'S NICKNAME" = mv.nickname
        -- 👇 INNER JOIN: Solo propiedades que realmente existen en Guesty
        JOIN guesty_listing gl ON r."LISTING'S NICKNAME" = gl.nickname
        WHERE 
            lower(trim(r."STATUS")) = 'confirmed'
            AND mv.offboarding_guesty IS NOT NULL
            AND gl.data ->> '_id' IS NOT NULL
            
            -- CONDICIÓN 1: Siguen ACTIVE en Guesty
            AND lower(trim(gl.data ->> 'active')) = 'true'
            
            -- CONDICIÓN 2: LA ALERTA (Check out es mayor al Blockoff)
            AND TO_DATE(regexp_replace(r."CHECK OUT", '[[:space:]]+', '', 'g'), 'DD/MM/YYYY') > mv.offboarding_guesty
            
        ORDER BY "check_out_date" ASC;
        """
        cur.execute(query)
        results = cur.fetchall()
        return results
    except Exception as e:
        logger.error(f"❌ Error Alertas Activas: {e}")
        return []
    finally:
        if cur: cur.close()
        if conn: conn.close()

# ==========================================
# 3️⃣ HOJA 3: SISTEMA REACTIVO (Reservas en Deptos Inactivos con Violación Histórica)
# ==========================================
def get_reactive_report():
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
            gl.data ->> 'active' AS "status_json",
            TO_DATE(regexp_replace(r."CHECK IN", '[[:space:]]+', '', 'g'), 'DD/MM/YYYY') AS "check_in_date",
            TO_DATE(regexp_replace(r."CHECK OUT", '[[:space:]]+', '', 'g'), 'DD/MM/YYYY') AS "check_out_date"
        FROM reservation_gold r
        JOIN mv_listings mv ON r."LISTING'S NICKNAME" = mv.nickname
        -- 👇 INNER JOIN: Solo propiedades que realmente existen en Guesty
        JOIN guesty_listing gl ON r."LISTING'S NICKNAME" = gl.nickname
        WHERE 
            lower(trim(r."STATUS")) = 'confirmed'
            AND mv.offboarding_guesty IS NOT NULL
            AND gl.data ->> '_id' IS NOT NULL
            
            -- CONDICIÓN 1: Ya están INACTIVE en Guesty (Registro Histórico)
            AND lower(trim(gl.data ->> 'active')) = 'false'
            
            -- CONDICIÓN 2: LA ALERTA (Check out fue mayor al Blockoff)
            AND TO_DATE(regexp_replace(r."CHECK OUT", '[[:space:]]+', '', 'g'), 'DD/MM/YYYY') > mv.offboarding_guesty
            
        ORDER BY "check_out_date" DESC;
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