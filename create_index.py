import os
import sys
import datetime
from sqlalchemy import create_engine, text

# --- CONFIGURACIÓN DE BASE DE DATOS ---
TABLE_NAME = 'p2p_anuncios'
DATABASE_URL = os.environ.get("DATABASE_URL")

if not DATABASE_URL:
    print(f"[{datetime.datetime.now()}] ERROR FATAL: No se encontró la variable de entorno DATABASE_URL.")
    sys.exit(1)

# Forzar prefijo 'postgresql://'
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

try:
    print(f"[{datetime.datetime.now()}] Conectando a la base de datos en Render...")
    ENGINE = create_engine(DATABASE_URL)
    with ENGINE.connect() as connection:
        print(f"[{datetime.datetime.now()}] Conexión exitosa.")
        
        # --- EL COMANDO MÁGICO ---
        # Esto crea un índice en la columna "Timestamp".
        # "IF NOT EXISTS" asegura que no dé error si ya lo ejecutaste.
        sql_command = text(f"""
            CREATE INDEX IF NOT EXISTS idx_timestamp 
            ON {TABLE_NAME} ("Timestamp");
        """)
        
        print(f"[{datetime.datetime.now()}] Ejecutando comando para crear índice... (Esto puede tardar unos segundos)")
        connection.execute(sql_command)
        # Necesitamos un 'commit' para que el CREATE INDEX se aplique
        connection.commit() 
        
        print(f"[{datetime.datetime.now()}] ¡Éxito! El índice 'idx_timestamp' ha sido creado o ya existía.")
        print(f"[{datetime.datetime.now()}] Tu Dashboard ('app.py') ahora debería cargar los datos mucho más rápido.")

except Exception as e:
    print(f"[{datetime.datetime.now()}] ❌ ERROR durante la creación del índice: {e}")
    sys.exit(1)
