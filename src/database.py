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
    VERSIÓN HÍBRIDA (SUPERIOR).
    Combina dos estrategias para no dejar escapar nada:
    1. OVERLAP: Detecta reservas que pisan CUALQUIER bloqueo 'BLOOFF' (histórico o actual).
    2. POST-OFFBOARDING: Detecta reservas posteriores al último cierre (lógica oficial).
    """
    conn = get_db_connection()
    if not conn: 
        logger.warning("⚠️ Saltando consulta por fallo de conexión.")
        return []

    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
        WITH cleaned_blocks AS (
            SELECT 
                nickname,
                code,
                -- Limpieza de fechas (igual que la MV oficial)
                TO_DATE(regexp_replace(check_in, '[[:space:]]+', '', 'g'), 'DD/MM/YYYY') as start_date,
                TO_DATE(regexp_replace(check_out, '[[:space:]]+', '', 'g'), 'DD/MM/YYYY') as end_date
            FROM block_gold 
            WHERE code = 'BLOOFF'
        ),
        max_dates AS (
            -- Calculamos también el último cierre para la lógica oficial
            SELECT nickname, MAX(start_date) as max_start 
            FROM cleaned_blocks 
            GROUP BY nickname
        )
        SELECT DISTINCT
            l.nickname AS property_name, 
            cb.code AS conflict_reason, 
            r.confirmation_code,
            r.status AS reservation_status,
            r.check_in AS res_check_in,
            r.check_out AS res_check_out,
            cb.start_date AS block_start,
            cb.end_date AS block_end,
            md.max_start AS last_offboarding
        FROM guesty_reservation r
        JOIN guesty_listing l ON r.listing_id = l.id
        JOIN cleaned_blocks cb ON l.nickname = cb.nickname
        LEFT JOIN max_dates md ON l.nickname = md.nickname
        WHERE 
            r.status IN ('confirmed', 'reserved')
            -- Filtro de acción: Solo queremos alertas vigentes hoy o a futuro
            AND r.check_out >= CURRENT_DATE 
            AND (
                -- LÓGICA 1: Solapamiento Físico (Atrapa lo que la MV ignora)
                (r.check_in < cb.end_date AND r.check_out > cb.start_date)
                
                OR
                
                -- LÓGICA 2: Post-Offboarding (Lógica oficial de la empresa)
                (r.check_in > md.max_start)
            )
        ORDER BY r.check_in ASC;
        """
        
        cur.execute(query)
        results = cur.fetchall()
        
        # Procesamiento para que el correo sea claro sobre CUAL de las dos razones fue
        final_results = []
        seen_codes = set()

        for row in results:
            code = row['confirmation_code']
            if code in seen_codes: continue # Evitar duplicados si cae en ambas lógicas
            seen_codes.add(code)

            # Determinamos la etiqueta para el correo
            if row['res_check_in'] > row['last_offboarding']:
                row['conflict_reason'] = f"POST-CIERRE ({row['last_offboarding']})"
            else:
                row['conflict_reason'] = "SOLAPAMIENTO"
            
            final_results.append(row)

        count = len(final_results)
        if count > 0:
            logger.warning(f"🚨 ALERTA HÍBRIDA: Se detectaron {count} conflictos (Overlap + Post-Cierre).")
        else:
            logger.info("✅ Todo limpio: El sistema híbrido no detectó conflictos.")
        
        cur.close()
        conn.close()
        return final_results

    except Exception as e:
        logger.error(f"❌ Error ejecutando Query Híbrida: {e}", exc_info=True)
        if conn: conn.close()
        return []