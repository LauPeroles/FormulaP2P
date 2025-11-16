import requests
import pandas as pd
from sqlalchemy import create_engine, text, inspect, Column, Integer, String, Float, DateTime, Text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
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

Base = declarative_base()
TABLE_NAME = 'p2p_anuncios'

# --- DEFINICIÓN DEL MODELO DE LA TABLA ---
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
                
                # Intentar crear el índice de Timestamp inmediatamente
                print(f"[{datetime.datetime.now()}] Creando índice 'idx_timestamp' en la nueva tabla...")
                sql_command = text(f'CREATE INDEX IF NOT EXISTS idx_timestamp ON {TABLE_NAME} ("Timestamp");')
                connection.execute(sql_command)
                connection.commit()
                print(f"[{datetime.datetime.now()}] Índice 'idx_timestamp' creado.")
            else:
                print(f"[{datetime.datetime.now()}] La tabla '{TABLE_NAME}' ya existe.")
    except Exception as e:
        print(f"[{datetime.datetime.now()}] ERROR durante la inicialización de la BD: {e}")

# --- CLASE PRINCIPAL DEL SCRAPER ---
class ScraperP2P:
    def __init__(self, engine):
        self.base_url = "https://api.p2p.co/api/v1/otc/market-order"
        self.session_db = sessionmaker(bind=engine)()
        self.total_registros_sesion = 0
        self.exchange_name = "ElDorado" # Nombre por defecto

    def _obtener_precio_base(self, tipo_anuncio):
        """Obtiene el precio de referencia para el filtro."""
        try:
            params = {
                "currency": "USDT",
                "paymentCurrency": "VES",
                "side": "BUY" if tipo_anuncio == "Demanda" else "SELL",
                "paymentMethodIds": [],
                "size": 1,
                "page": 1
            }
            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            if data and data['result'] and data['result']['list']:
                precio_base = float(data['result']['list'][0]['price'])
                # Intentar obtener el nombre de la exchange
                if 'exchangeName' in data['result']['list'][0]:
                    self.exchange_name = data['result']['list'][0]['exchangeName']
                return precio_base
        except requests.RequestException as e:
            print(f"<i>[!] Advertencia: No se pudo obtener el precio base. {e}</i>")
        return None

    def _aplicar_filtro_inteligente(self, precio_base, tipo_anuncio):
        """Devuelve el rango de precios aceptable."""
        if precio_base is None:
            return None, None
        
        # Rango de filtro (ej. 12%)
        rango = 0.12 
        if tipo_anuncio == "Demanda":
            precio_min = precio_base * (1 - rango)
            precio_max = precio_base * (1 + rango)
        else: # Oferta
            precio_min = precio_base * (1 - rango)
            precio_max = precio_base * (1 + rango)
        return precio_min, precio_max

    def obtener_anuncios(self, tipo_anuncio):
        """Obtiene y filtra los anuncios de la API."""
        print(f"  → Obteniendo datos de {tipo_anuncio}...")
        side = "BUY" if tipo_anuncio == "Demanda" else "SELL"
        
        precio_base = self._obtener_precio_base(tipo_anuncio)
        p_min, p_max = self._aplicar_filtro_inteligente(precio_base, tipo_anuncio)
        
        if p_min is None:
            print(f"<i>[!] No se pudo aplicar filtro para {tipo_anuncio}. Saltando...</i>")
            return [], 0
            
        print(f"<i>   <i-> Precio base ({tipo_anuncio}): {precio_base:.2f}. Rango aceptado: ({p_min:.2f} - {p_max:.2f})</i>")

        params = {
            "currency": "USDT",
            "paymentCurrency": "VES",
            "side": side,
            "paymentMethodIds": [],
            "size": 200, # Pedimos 200 para tener un buen set de datos
            "page": 1
        }
        
        try:
            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            anuncios_filtrados = []
            if data and data['result'] and data['result']['list']:
                timestamp = datetime.datetime.now()
                
                for item in data['result']['list']:
                    try:
                        precio = float(item['price'])
                        
                        # El filtro de precios
                        if not (p_min <= precio <= p_max):
                            continue # El precio está fuera de rango, lo ignoramos

                        volumen = float(item['amount'])
                        volumen_min = float(item['minLimit'])
                        volumen_max = float(item['maxLimit'])
                        metodos_pago_lista = [pm['paymentMethodName'] for pm in item.get('paymentMethods', [])]
                        metodos_pago_str = ', '.join(metodos_pago_lista)

                        anuncio_obj = Anuncio(
                            Timestamp=timestamp,
                            Tipo=tipo_anuncio,
                            Precio=precio,
                            Volumen=volumen,
                            Volumen_min=volumen_min,
                            Volumen_max=volumen_max,
                            Metodos_Pago=metodos_pago_str,
                            Exchange_Name=self.exchange_name
                        )
                        anuncios_filtrados.append(anuncio_obj)
                    except (ValueError, TypeError) as e:
                        print(f"<i>   [!] Error procesando un anuncio: {e}. Saltando...</i>")
                        
                print(f"<i>   <i> Anuncios de {tipo_anuncio} recolectados: {len(anuncios_filtrados)}</i>")
                return anuncios_filtrados, len(anuncios_filtrados)
            else:
                print(f"<i>   <i> No se encontraron anuncios de {tipo_anuncio}.</i>")
                return [], 0
                
        except requests.RequestException as e:
            print(f"<i>   [!] Error de red obteniendo {tipo_anuncio}: {e}</i>")
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

    def ejecutar_ciclo(self):
        """Ejecuta un ciclo completo de recolección."""
        print(f"--- Iniciando ciclo de extracción a las {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
        
        anuncios_demanda, count_d = self.obtener_anuncios("Demanda")
        anuncios_oferta, count_o = self.obtener_anuncios("Oferta")
        
        todos_anuncios = anuncios_demanda + anuncios_oferta
        self.guardar_en_db(todos_anuncios)
        
        total_nuevos = count_d + count_o
        self.total_registros_sesion += total_nuevos
        
        print(f"  📊 \x1b[1;32m¡Éxito! {total_nuevos} nuevos registros añadidos a la BD.\x1b[0m")
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
