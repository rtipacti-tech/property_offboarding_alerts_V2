import os
import psycopg2
from dotenv import load_dotenv

# 1. Cargar el archivo .env
load_dotenv()

print("--- 📡 INICIANDO PRUEBA DE CONEXIÓN ---")
print(f"Intentando conectar a:")
print(f"Host: {os.getenv('DB_HOST')}")
print(f"Base de Datos: {os.getenv('DB_NAME')}")
print(f"Usuario: {os.getenv('DB_USER')}")
print("---------------------------------------")

try:
    # 2. Intentar conectar
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        port=os.getenv("DB_PORT")
    )
    
    # 3. Si llega aquí, es que conectó. Hacemos una consulta simple.
    cur = conn.cursor()
    cur.execute("SELECT version();")
    db_version = cur.fetchone()
    
    print("\n✅ ¡ÉXITO TOTAL!")
    print("La conexión funciona perfectamente.")
    print(f"Versión de Postgres detectada: {db_version[0]}")
    
    cur.close()
    conn.close()

except Exception as e:
    print("\n❌ ERROR DE CONEXIÓN:")
    print("No se pudo conectar. Revisa el mensaje de error abajo:")
    print(f"Detalle del error: {e}")