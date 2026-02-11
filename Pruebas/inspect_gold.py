import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger()

load_dotenv()

def inspect_fast():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            port=os.getenv("DB_PORT")
        )
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Las 3 tablas que me mencionaste
        TARGETS = ['block_gold', 'block_silver', 'block_silver_flag']
        
        logger.info("🚀 ESCANEO DE ESTRUCTURA (METADATA ONLY)...\n")

        for table in TARGETS:
            logger.info(f"📂 TABLA: {table}")
            
            # 1. Buscamos columnas en el esquema público
            cur.execute(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = '{table}';
            """)
            cols = cur.fetchall()

            if not cols:
                # Si no aparece en information_schema, a veces las MVs están solo en pg_matviews
                # Probamos un truco para ver si existe aunque sea
                cur.execute(f"SELECT count(*) FROM pg_matviews WHERE matviewname = '{table}';")
                es_mv = cur.fetchone()
                if es_mv and es_mv['count'] > 0:
                    logger.info("   ⚠️ Es una Vista Materializada compleja (no mostró columnas en esquema estándar).")
                    # Truco ninja: Leemos 0 filas para sacar los encabezados
                    try:
                        cur.execute(f"SELECT * FROM {table} LIMIT 0;")
                        col_names = [desc[0] for desc in cur.description]
                        logger.info(f"   📝 Columnas detectadas: {col_names}")
                    except Exception as e:
                        logger.error(f"   ❌ No se pudo leer: {e}")
                else:
                    logger.error("   ❌ No se encontró la tabla/vista.")
            else:
                # Si encontró columnas normal
                col_names = [f"{c['column_name']} ({c['data_type']})" for c in cols]
                for c in col_names:
                    logger.info(f"   🔹 {c}")

            logger.info("-" * 40)

        cur.close()
        conn.close()

    except Exception as e:
        logger.error(f"❌ Error: {e}")

if __name__ == "__main__":
    inspect_fast()