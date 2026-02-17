import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

def inspeccionar_mv():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            port=os.getenv("DB_PORT")
        )
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        TABLE_NAME = "mv_listings"
        
        print(f"\n🕵️‍♂️ INSPECCIONANDO LA VISTA MAESTRA: '{TABLE_NAME}'...\n")

        # 1. BUSCAR LA COLUMNA EXACTA
        print("--- 1. BUSCANDO COLUMNAS 'OFFBOARD' ---")
        cur.execute(f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = '{TABLE_NAME}'
            AND column_name ILIKE '%offboard%';
        """)
        cols = cur.fetchall()

        target_col = None
        if cols:
            for c in cols:
                print(f"   ✅ Encontrada: {c['column_name']} ({c['data_type']})")
                # Si encontramos la que dijiste, la guardamos para probarla
                if 'guesty' in c['column_name'] or 'date' in c['column_name']:
                    target_col = c['column_name']
        else:
            print("   ❌ No aparecen columnas con 'offboard' en information_schema.")
            print("      (A veces las MVs no salen ahí. Probaremos suerte consultando directo).")
            target_col = "offboarding_guesty" # Asumimos el nombre que me diste

        # 2. VER DATOS REALES
        if target_col:
            print(f"\n--- 2. MUESTRA DE DATOS: {target_col} ---")
            try:
                # Traemos datos NO NULOS para ver el formato real
                query = f"SELECT nickname, {target_col} FROM {TABLE_NAME} WHERE {target_col} IS NOT NULL LIMIT 10;"
                cur.execute(query)
                rows = cur.fetchall()
                
                if rows:
                    for r in rows:
                        print(f"   👉 {r['nickname']}: {r[target_col]}")
                else:
                    print("   ⚠️ La columna existe pero está vacía.")
            except Exception as e:
                print(f"   ❌ Error al leer datos: {e}")
        
        cur.close()
        conn.close()

    except Exception as e:
        print(f"❌ Error crítico: {e}")

if __name__ == "__main__":
    inspeccionar_mv()