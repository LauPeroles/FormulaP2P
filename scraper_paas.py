import requests
import pandas as pd
from sqlalchemy import create_engine, text, inspect, Column, Integer, String, Float, DateTime, Text
# Corrección de importación para SQLAlchemy 2.0
from sqlalchemy.orm import sessionmaker, declarative_base 
import time
import datetime
import os
import sys

# --- CONFIGURACIÓN DE BASE DE DATOS ---
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    print(f"[{datetime.datetime.now()}] ERROR FATAL: No se encontró la variable de entorno DATABASE_URL.")
    sys.exit(1)

# Forzar prefijo 'postgresql://'
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

try:
    ENGINE = create_engine(DATABASE_URL)
except Exception as e:
    print(f"[{datetime.datetime.now()}] ERROR FATAL: No se pudo crear engine de SQLAlchemy: {e}")
    sys.exit(1)

# Ajuste para SQLAlchemy 2.0
Base = declarative_base()
TABLE_NAME = 'p2p_anuncios'

# --- DEFINICIÓN DEL MODELO DE LA TABLA ---
# (Este modelo no cambia, es compatible con ambos scrapers)
class Anuncio(Base):
    __tablename__ = TABLE_NAME
    id = Column(Integer, primary_key=True)
    Timestamp = Column(DateTime, nullable=False, index=True)
    Tipo = Column(String(10), nullable=False)
    Precio = Column(Float, nullable=False)
    Volumen = Column(Float, nullable=False)
    Volumen_min = Column(Float)
    Volumen_max = Column(Float)
    Metodos_Pago = Column(Text)
    Exchange_Name = Column(String(50))

# --- FUNCIÓN PARA CREAR LA TABLA (si no existe) ---
def inicializar_base_de_datos():
    try:
        with ENGINE.connect() as connection:
            inspector = inspect(ENGINE)
            if not inspector.has_table(TABLE_NAME):
                print(f"[{datetime.datetime.now()}] Creando tabla '{TABLE_NAME}' por primera vez...")
                Base.metadata.create_all(ENGINE)
                print(f"[{datetime.datetime.now()}] Tabla '{TABLE_NAME}' creada con éxito.")
                
                print(f"[{datetime.datetime.now()}] Creando índice 'idx_timestamp' en la nueva tabla...")
                sql_command = text(f'CREATE INDEX IF NOT EXISTS idx_timestamp ON {TABLE_NAME} ("Timestamp");')
                connection.execute(sql_command)
                connection.commit()
                print(f"[{datetime.datetime.now()}] Índice 'idx_timestamp' creado.")
            else:
                print(f"[{datetime.datetime.now()}] La tabla '{TABLE_NAME}' ya existe.")
    except Exception as e:
        print(f"[{datetime.datetime.now()}] ERROR durante la inicialización de la BD: {e}")

# --- CLASE PRINCIPAL DEL SCRAPER (ADAPTADA A BINANCE) ---
class ScraperP2P:
    def __init__(self, engine):
        # --- ¡ESTA ES LA API CORRECTA DE BINANCE! ---
        self.base_url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
        self.session_db = sessionmaker(bind=engine)()
        self.total_registros_sesion = 0
        self.exchange_name = "Binance" # Nombre correcto
        
        # --- HEADERS ESENCIALES PARA BINANCE ---
        # Binance bloquea peticiones sin un User-Agent (como las de Render)
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
            'Content-Type': 'application/json',
            'Accept': '*/*',
            'Host': 'p2p.binance.com',
            'Origin': 'https://p2p.binance.com'
        }

    def obtener_anuncios(self, tipo_anuncio):
        """Obtiene y filtra los anuncios de la API de Binance."""
        print(f"  → Obteniendo datos de {tipo_anuncio} (Binance)...")
        
        # El "side" en la API de Binance es "tradeType"
        trade_type = "BUY" if tipo_anuncio == "Demanda" else "SELL"
        
        # --- PAYLOAD PARA LA PETICIÓN POST DE BINANCE ---
        payload = {
            "asset": "USDT",
            "fiat": "VES",
            "merchantCheck": False, # No incluir solo comerciantes
            "page": 1,
            "rows": 20, # 20 es el máximo de la API "friendly"
            "tradeType": trade_type,
            "payTypes": [], # Todos los métodos de pago
        }
        
        try:
            with requests.Session() as s:
                # --- ¡ES UN POST, NO UN GET! ---
                response = s.post(self.base_url, headers=self.headers, json=payload, timeout=10)
            
            response.raise_for_status() # Lanza error si la respuesta es 4xx o 5xx
            data = response.json()
            
            anuncios_guardar = []
            
            # La respuesta de Binance tiene un formato específico
            if data and data.get('success') and data.get('data'):
                timestamp = datetime.datetime.now()
                
                for item in data['data']:
                    try:
                        # Los datos del anuncio están en el sub-diccionario 'adv'
                        adv = item['adv']
                        
                        precio = float(adv['price'])
                        volumen = float(adv['surplusAmount']) # 'surplusAmount' es el volumen disponible
                        volumen_min = float(adv['minSingleTransAmount'])
                        volumen_max = float(adv['maxSingleTransAmount'])
                        
                        # Extraer métodos de pago
                        metodos_pago_lista = [pm['payType'] for pm in adv.get('tradeMethods', []) if pm.get('payType')]
                        metodos_pago_str = ', '.join(metodos_pago_lista)

                        anuncio_obj = Anuncio(
                            Timestamp=timestamp,
                            Tipo=tipo_anuncio,
                            Precio=precio,
                            Volumen=volumen,
                            Volumen_min=volumen_min,
                            Volumen_max=volumen_max,
                            Metodos_Pago=metodos_pago_str,
                            Exchange_Name=self.exchange_name # Guardamos "Binance"
                        )
                        anuncios_guardar.append(anuncio_obj)
                    except (ValueError, TypeError, KeyError) as e:
                        print(f"<i>   [!] Error procesando un anuncio de Binance: {e}. Saltando...</i>")
                        
                print(f"<i>   <i> Anuncios de {tipo_anuncio} recolectados: {len(anuncios_guardar)}</i>")
                return anuncios_guardar, len(anuncios_guardar)
            
            else:
                print(f"<i>   <i> No se encontraron anuncios de {tipo_anuncio} o la respuesta no fue exitosa.</i>")
                return [], 0
                
        except requests.RequestException as e:
            print(f"<i>   [!] Error de red obteniendo {tipo_anuncio} (Binance): {e}</i>")
            return [], 0
        except Exception as e:
            print(f"<i>   [!] Error inesperado en obtener_anuncios (Binance): {e}</i>")
            return [], 0

    def guardar_en_db(self, anuncios):
        """Guarda la lista de anuncios en la base de datos."""
        if not anuncios:
            return
        try:
            self.session_db.add_all(anuncios)
            self.session_db.commit()
        except Exception as e:
            print(f"<i>[!] Error al guardar en BD: {e}</i>")
            self.session_db.rollback()
        finally:
            self.session_db.close() # Cerrar sesión después de cada ciclo

    def ejecutar_ciclo(self):
        """Ejecuta un ciclo completo de recolección."""
        print(f"--- Iniciando ciclo de extracción a las {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
        
        anuncios_demanda, count_d = self.obtener_anuncios("Demanda")
        anuncios_oferta, count_o = self.obtener_anuncios("Oferta")
        
        todos_anuncios = anuncios_demanda + anuncios_oferta
        self.guardar_en_db(todos_anuncios)
        
        total_nuevos = count_d + count_o
        self.total_registros_sesion += total_nuevos
        
        if total_nuevos > 0:
            print(f"  📊 \x1b[1;32m¡Éxito! {total_nuevos} nuevos registros añadidos a la BD.\x1b[0m")
        else:
            print(f"  📊 ¡Éxito! {total_nuevos} nuevos registros añadidos a la BD.")
            
        print(f"  ⭐ Total acumulado en esta sesión: {self.total_registros_sesion} registros.")
        print(f"-----------------------------------------------------------------")

# --- PUNTO DE ENTRADA DEL SCRIPT ---
if __name__ == "__main__":
    
    # Asegurarse de que la tabla exista antes de empezar
    inicializar_base_de_datos()
    
    scraper = ScraperP2P(ENGINE)
    
    # Este es el modo "Cron Job": se ejecuta UNA VEZ y termina.
    # Render lo llamará cada 2 minutos.
    try:
        scraper.ejecutar_ciclo()
        print(f"[{datetime.datetime.now()}] Ciclo único completado. Saliendo.")
        sys.exit(0) # Salida limpia
    except Exception as e:
        print(f"[{datetime.datetime.now()}] ERROR INESPERADO en el ciclo principal: {e}")
        sys.exit(1) # Salida con error
