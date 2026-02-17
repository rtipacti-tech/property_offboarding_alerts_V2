import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import logging
from datetime import datetime, date

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

# --- LISTA 1: CONFLICTOS DE RESERVAS (La que ya validamos) ---
def find_orphaned_bookings():
    """
    Busca reservas que chocan con bloqueos físicos o fechas de cierre.
    """
    conn = get_db_connection()
    if not conn: return []

    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
        WITH cleaned_blocks AS (
            SELECT 
                nickname,
                TO_DATE(regexp_replace(check_in, '[[:space:]]+', '', 'g'), 'DD/MM/YYYY') as start_date,
                TO_DATE(regexp_replace(check_out, '[[:space:]]+', '', 'g'), 'DD/MM/YYYY') as end_date
            FROM block_gold 
            WHERE code = 'BLOOFF'
        ),
        last_offboarding AS (
            SELECT DISTINCT ON (nickname) 
                nickname, start_date, end_date
            FROM cleaned_blocks
            ORDER BY nickname, start_date DESC
        )
        SELECT DISTINCT
            l.nickname AS property_name, 
            r.confirmation_code,
            r.status AS reservation_status,
            r.check_in AS res_check_in,
            r.check_out AS res_check_out,
            cb.start_date AS block_start,
            lo.start_date AS last_off_start
        FROM guesty_reservation r
        JOIN guesty_listing l ON r.listing_id = l.id
        JOIN cleaned_blocks cb ON l.nickname = cb.nickname
        LEFT JOIN last_offboarding lo ON l.nickname = lo.nickname
        WHERE 
            r.status IN ('confirmed', 'reserved')
            AND (
                (r.check_in < cb.end_date AND r.check_out > cb.start_date)
                OR
                (r.check_in > lo.start_date AND lo.end_date >= CURRENT_DATE AND r.listing_id = l.id)
            )
        ORDER BY r.check_in ASC;
        """
        
        cur.execute(query)
        results = cur.fetchall()
        
        # Procesamiento de etiquetas (Igual que antes)
        final_results = []
        seen_codes = set()
        for row in results:
            code = row['confirmation_code']
            if code in seen_codes: continue 
            seen_codes.add(code)

            res_start = row['res_check_in']
            last_off_start = row['last_off_start']
            
            if row['res_check_out'] < date.today():
                row['conflict_reason'] = "HISTÓRICO: POST-CIERRE" if res_start > last_off_start else "HISTÓRICO: OVERLAP"
            else:
                row['conflict_reason'] = f"ACTIVO: POST-CIERRE ({last_off_start})" if res_start > last_off_start else "ACTIVO: OVERLAP"

            final_results.append(row)
        
        cur.close()
        conn.close()
        return final_results

    except Exception as e:
        logger.error(f"❌ Error en Query Auditoría Reservas: {e}", exc_info=True)
        if conn: conn.close()
        return []

# --- LISTA 2: PROPIEDADES EN OFFBOARDING OFICIAL (Lo nuevo de tu jefe) ---
def find_offboarding_listings():
    """
    Busca propiedades que tienen fecha en 'offboarding_guesty' (mv_listings).
    Filtro: Fechas de los últimos 30 días O fechas futuras.
    """
    conn = get_db_connection()
    if not conn: return []

    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Consulta limpia y directa a mv_listings
        query = """
        SELECT 
            nickname AS property_name,
            offboarding_guesty AS offboarding_date,
            country,
            city
        FROM mv_listings
        WHERE 
            offboarding_guesty IS NOT NULL
            AND (
                -- Opción A: Fecha futura (ej. se va el próximo mes)
                offboarding_guesty >= CURRENT_DATE
                OR
                -- Opción B: Fecha reciente (se fue hace menos de 30 días)
                offboarding_guesty >= (CURRENT_DATE - INTERVAL '30 days')
            )
        ORDER BY offboarding_guesty ASC;
        """
        
        cur.execute(query)
        results = cur.fetchall()
        
        count = len(results)
        if count > 0:
            logger.info(f"📋 LISTA OFICIAL: Se encontraron {count} propiedades con fecha de corte reciente/futura.")
        
        cur.close()
        conn.close()
        return results

    except Exception as e:
        logger.error(f"❌ Error en Query Lista Oficial: {e}", exc_info=True)
        if conn: conn.close()
        return []