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

def find_orphaned_bookings():
    """
    Busca reservas confirmadas que ocurren DESPUÉS de la fecha de inicio del Offboarding.
    Lógica alineada con la MV 'reservations_post_blooff':
    1. Calcula la fecha MÁXIMA de inicio de un BLOOFF por propiedad.
    2. Busca reservas cuyo Check-in sea MAYOR a esa fecha.
    3. Filtra solo las que son relevantes HOY (Check-out futuro).
    """
    conn = get_db_connection()
    if not conn: 
        logger.warning("⚠️ Saltando consulta por fallo de conexión.")
        return []

    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Usamos una CTE (Common Table Expression) para replicar la lógica de la MV
        # pero aplicándola sobre datos en tiempo real.
        query = """
        WITH blooff_max AS (
            SELECT
                nickname,
                MAX(
                    CASE
                        -- Limpieza de espacios y conversión segura a fecha, igual que la MV
                        WHEN regexp_replace(check_in, '[[:space:]]+', '', 'g') ~ '^\d{1,2}/\d{1,2}/\d{4}$'
                        THEN to_date(regexp_replace(check_in, '[[:space:]]+', '', 'g'), 'DD/MM/YYYY')
                        ELSE NULL
                    END
                ) AS max_blooff_ci
            FROM block_gold
            WHERE code = 'BLOOFF'
            GROUP BY nickname
        )
        SELECT 
            l.nickname AS property_name, 
            'BLOOFF' AS conflict_reason, 
            r.confirmation_code,
            r.status AS reservation_status,
            r.check_in AS res_check_in,
            r.check_out AS res_check_out,
            bm.max_blooff_ci AS offboarding_date
        FROM guesty_reservation r
        JOIN guesty_listing l ON r.listing_id = l.id
        JOIN blooff_max bm ON l.nickname = bm.nickname
        WHERE 
            -- 1. Solo reservas confirmadas o reservadas
            r.status IN ('confirmed', 'reserved')
            
            -- 2. FILTRO DE TIEMPO (Lógica de la Empresa):
            -- La reserva empieza DESPUÉS de que se activó el Offboarding
            AND r.check_in > bm.max_blooff_ci
            
            -- 3. FILTRO DE ACCIÓN (Lógica para la Alerta):
            -- Solo queremos ver problemas activos hoy o en el futuro.
            -- (Si quitamos esto, te saldrían las de Enero que viste en la tabla)
            AND r.check_out >= CURRENT_DATE
        
        ORDER BY r.check_in ASC;
        """
        
        cur.execute(query)
        results = cur.fetchall()
        
        count = len(results)
        if count > 0:
            logger.warning(f"🚨 ALERTA: Se detectaron {count} reservas posteriores al Offboarding.")
        else:
            logger.info("✅ Todo limpio: No hay reservas nuevas en propiedades offboardeadas.")
        
        cur.close()
        conn.close()
        return results

    except Exception as e:
        logger.error(f"❌ Error ejecutando Query Alineada: {e}", exc_info=True)
        if conn: conn.close()
        return []