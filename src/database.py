import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

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
        print(f"❌ Error de conexión: {e}")
        return None

def find_orphaned_bookings():
    """
    Recupera reservas activas en propiedades inactivas.
    Filtro estricto: Status Inactivo + Reserva Futura/Presente.
    """
    conn = get_db_connection()
    if not conn: return []

    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Traemos Check-in Y Check-out para saber la duración del conflicto
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
                l.status = 'inactive'  -- DETECTA EL OFFBOARDING
                AND r.status IN ('confirmed', 'reserved') -- SOLO RESERVAS REALES
                AND r.check_in >= CURRENT_DATE -- SOLO FUTURAS O DE HOY
            ORDER BY r.check_in ASC;
        """
        
        cur.execute(query)
        results = cur.fetchall()
        cur.close()
        conn.close()
        return results

    except Exception as e:
        print(f"❌ Error SQL: {e}")
        if conn: conn.close()
        return []