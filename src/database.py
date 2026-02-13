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

def find_orphaned_bookings():
    """
    MODO AUDITORÍA TOTAL (PASADO, PRESENTE Y FUTURO)
    
    Analiza reservas confirmadas que violan reglas de Offboarding.
    1. OVERLAP HISTÓRICO: ¿Hubo reservas encima de un BLOOFF en el pasado?
    2. POST-CIERRE VIGENTE: ¿Hay reservas futuras después del cierre definitivo?
    """
    conn = get_db_connection()
    if not conn: return []

    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
        WITH cleaned_blocks AS (
            -- 1. Traemos TODOS los BLOOFF de la historia (limpios desde block_gold)
            SELECT 
                nickname,
                TO_DATE(regexp_replace(check_in, '[[:space:]]+', '', 'g'), 'DD/MM/YYYY') as start_date,
                TO_DATE(regexp_replace(check_out, '[[:space:]]+', '', 'g'), 'DD/MM/YYYY') as end_date
            FROM block_gold 
            WHERE code = 'BLOOFF'
        ),
        last_offboarding AS (
            -- 2. Calculamos el ÚLTIMO cierre conocido por propiedad
            SELECT DISTINCT ON (nickname) 
                nickname,
                start_date,
                end_date
            FROM cleaned_blocks
            ORDER BY nickname, start_date DESC
        )
        SELECT DISTINCT
            l.nickname AS property_name, 
            'ANALIZANDO...' AS conflict_reason, 
            r.confirmation_code,
            r.status AS reservation_status,
            r.check_in AS res_check_in,
            r.check_out AS res_check_out,
            cb.start_date AS block_start,
            cb.end_date AS block_end,
            lo.start_date AS last_off_start,
            lo.end_date AS last_off_end
        FROM guesty_reservation r
        JOIN guesty_listing l ON r.listing_id = l.id
        -- Hacemos JOIN con todos los bloques para detectar choques históricos
        JOIN cleaned_blocks cb ON l.nickname = cb.nickname
        -- Hacemos JOIN con el último cierre para detectar fugas futuras
        LEFT JOIN last_offboarding lo ON l.nickname = lo.nickname
        WHERE 
            r.status IN ('confirmed', 'reserved')
            
            AND (
                -- CASO 1: SOLAPAMIENTO DIRECTO (Histórico o Futuro)
                -- La reserva pisa físicamente un bloque BLOOFF existente en block_gold.
                (r.check_in < cb.end_date AND r.check_out > cb.start_date)
                
                OR
                
                -- CASO 2: POST-CIERRE DEFINITIVO (Lógica MV Oficial)
                -- Solo aplica si el Último Offboarding sigue vigente (termina en el futuro o es largo)
                -- y la reserva es posterior a ese inicio.
                (
                    r.check_in > lo.start_date 
                    AND lo.end_date >= CURRENT_DATE 
                    AND r.listing_id = l.id -- asegurar consistencia
                )
            )
            
        ORDER BY r.check_in ASC;
        """
        
        cur.execute(query)
        results = cur.fetchall()
        
        # Procesamiento final para etiquetar correctamente en el correo
        final_results = []
        seen_codes = set()

        for row in results:
            code = row['confirmation_code']
            if code in seen_codes: continue 
            seen_codes.add(code)

            # Lógica de Etiquetado para que entiendas qué pasó
            res_start = row['res_check_in']
            block_start = row['block_start']
            last_off_start = row['last_off_start']
            
            # Si es pasado (ya ocurrió)
            if row['res_check_out'] < date.today():
                if res_start > last_off_start:
                    row['conflict_reason'] = "HISTÓRICO: POST-CIERRE"
                else:
                    row['conflict_reason'] = "HISTÓRICO: OVERLAP"
            
            # Si es futuro (Alerta accionable)
            else:
                if res_start > last_off_start:
                    row['conflict_reason'] = f"ACTIVO: POST-CIERRE ({last_off_start})"
                else:
                    row['conflict_reason'] = "ACTIVO: OVERLAP"

            final_results.append(row)

        count = len(final_results)
        if count > 0:
            logger.warning(f"🚨 AUDITORÍA COMPLETA: Se detectaron {count} conflictos (Pasados y Futuros).")
        else:
            logger.info("✅ Propiedad Inmaculada: No hay conflictos ni históricos ni futuros.")
        
        cur.close()
        conn.close()
        return final_results

    except Exception as e:
        logger.error(f"❌ Error en Query Auditoría: {e}", exc_info=True)
        if conn: conn.close()
        return []