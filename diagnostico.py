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
        print(f"❌ Error conectando: {e}")
        return None

def escaner_rapido():
    conn = get_db_connection()
    if not conn: return

    cur = conn.cursor(cursor_factory=RealDictCursor)
    print("\n🚀 INICIANDO ESCANEO RÁPIDO (SOLO METADATA)...\n")

    # 1. BUSCAR TODAS LAS TABLAS QUE TENGAN "GUESTY" EN EL NOMBRE
    # Esto nos dirá si existe una tabla llamada 'guesty_blocks' o 'guesty_calendar'
    print("--- 📂 TABLAS ENCONTRADAS ---")
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name LIKE '%guesty%'
        ORDER BY table_name;
    """)
    tablas = cur.fetchall()

    nombres_tablas = [t['table_name'] for t in tablas]
    
    if not nombres_tablas:
        print("⚠️ No se encontraron tablas con 'guesty' en el nombre.")
    else:
        for t in nombres_tablas:
            print(f"   📄 {t}")

    print("\n" + "="*30 + "\n")

    # 2. ANALIZAR COLUMNAS CLAVE DE LAS TABLAS MÁS IMPORTANTES
    # Solo miraremos columnas, NO datos. Es instantáneo.
    tablas_a_investigar = [
        'guesty_reservation', 
        'guesty_listing', 
        'guesty_calendar_block', # Adivinando nombres comunes
        'guesty_blocks'
    ]
    
    # Agregamos cualquier tabla que tenga la palabra "block" que hayamos encontrado arriba
    for t in nombres_tablas:
        if 'block' in t and t not in tablas_a_investigar:
            tablas_a_investigar.append(t)

    print("--- 🔍 INSPECCIONANDO COLUMNAS CLAVE ---")
    
    for tabla in tablas_a_investigar:
        # Verificamos si la tabla existe en la lista que encontramos
        if tabla in nombres_tablas:
            print(f"\n📂 TABLA: {tabla}")
            cur.execute(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = '{tabla}'
                ORDER BY ordinal_position;
            """)
            col_info = cur.fetchall()
            col_names = [c['column_name'] for c in col_info]
            
            # Imprimimos las columnas para que tú veas si hay algo como 'status', 'type', 'start_date'
            print(f"   📝 Columnas: {', '.join(col_names)}")
        else:
            # Si la tabla no existe (ej. guesty_blocks no existe), no pasa nada
            pass

    cur.close()
    conn.close()
    print("\n✅ Escaneo finalizado.")

if __name__ == "__main__":
    escaner_rapido()