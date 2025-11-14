import requests
import pandas as pd
from datetime import datetime
import time 
from sqlalchemy import create_engine 
import os 
import traceback 

# --- 1. CONFIGURACIÓN DE LA BASE DE DATOS (¡MODIFICADA PARA PAAS!) ---
try:
    DATABASE_URL = os.environ.get('DATABASE_URL')
    if not DATABASE_URL:
        raise ValueError("No se encontró la variable de entorno DATABASE_URL")
    
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
        
    engine = create_engine(DATABASE_URL)
    TABLE_NAME = 'p2p_anuncios'
    print("Scraper conectado a la base de datos PostgreSQL.")
except Exception as e:
    print(f"❌ ERROR: Scraper no pudo conectarse a la BD. Deteniendo. {e}")
    engine = None # Detenemos el script si no hay BD

# --- LÍMITES DEL FILTRO DINÁMICO ---
TOLERANCIA_PRECIO = 0.10 # 10% de tolerancia

# --- 2. FUNCIÓN DE GUARDADO (Sin cambios) ---
def guardar_en_bd(df_nuevos_registros):
    global engine, TABLE_NAME
    try:
        df_nuevos_registros.to_sql(
            TABLE_NAME,
            con=engine,
            if_exists='append', 
            index=False,
            method='multi' # Optimizado para inserciones masivas
        )
    except Exception as e:
        raise Exception(f"❌ Error al guardar en la BD: {e}")

# --- 3. FUNCIÓN DE EXTRACCIÓN (Sin cambios) ---
def obtener_datos_p2p(pagina, trade_type):
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    headers = {
        'Content-Type': 'application/json',
        'clientType': 'web',
    }
    payload = {
        "page": pagina,
        "rows": 20,
        "payTypes": [],
        "asset": "USDT",
        "fiat": "VES",
        "tradeType": trade_type,
    }
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10) # 10 seg de timeout
        response.raise_for_status()
        data = response.json()
        return data.get('data', [])
    except requests.exceptions.RequestException as e:
        print(f"❌ Error al obtener datos de la página {pagina} ({trade_type}): {e}")
        return []

# --- 4. FUNCIÓN DE PROCESAMIENTO (Sin cambios) ---
def procesar_datos(raw_data, fecha_hora, tipo_etiqueta, precio_min, precio_max):
    datos_limpios = []
    registros_rechazados = 0 
    
    for ad in raw_data:
        ad_info = ad.get('adv', {})
        precio = float(ad_info.get('price', 0))

        if precio < precio_min or precio > precio_max:
            registros_rechazados += 1
            continue 

        trade_methods = ad_info.get('tradeMethods', [])
        pay_methods = [method.get('payType') for method in trade_methods if method.get('payType')]
        if not pay_methods:
            pay_method_list = ad.get('collector', {}).get('payMethodList', [])
            pay_methods = [method.get('payType') for method in pay_method_list if method.get('payType')]
        
        datos_limpios.append({
            'Timestamp': fecha_hora,
            'Tipo': tipo_etiqueta,
            'Precio': precio, 
            'Volumen': float(ad_info.get('tradableQuantity', 0)),
            'Metodos_Pago': ', '.join(pay_methods),
        })
        
    if registros_rechazados > 0 and len(raw_data) > 0:
        precio_lote = float(raw_data[0].get('adv', {}).get('price', 0))
        if precio_lote >= precio_min and precio_lote <= precio_max:
             print(f"  <i>- (Filtro): {registros_rechazados} anuncios rechazados por precio fuera de rango ({precio_min:.2f} - {precio_max:.2f}).</i>")
        
    return datos_limpios

# --- 5. FUNCIÓN PRINCIPAL (Sin cambios) ---
def ejecutar_extraccion(num_paginas=10):
    
    fecha_hora_actual = datetime.now() # Usamos el objeto datetime
    print(f"\n--- Iniciando ciclo de extracción a las {fecha_hora_actual.strftime('%Y-%m-%d %H:%M:%S')} ---")

    datos_por_tipo = {}

    for trade_type in ["SELL", "BUY"]:
        todos_los_datos_tipo = []
        tipo_etiqueta = "Demanda" if trade_type == "SELL" else "Oferta"
        print(f"→ Obteniendo datos de {tipo_etiqueta}...")

        raw_page_1 = obtener_datos_p2p(1, trade_type)
        
        if not raw_page_1:
            print(f"  <i>- No se pudo obtener la página 1 para {tipo_etiqueta}. Saltando...</i>")
            continue

        try:
            precio_base = float(raw_page_1[0].get('adv', {}).get('price', 0))
            if precio_base == 0:
                raise Exception("Precio base es 0")
                
            precio_max_logico = precio_base * (1 + TOLERANCIA_PRECIO)
            precio_min_logico = precio_base * (1 - TOLERANCIA_PRECIO)
            
            print(f"  <i>- Precio base ({tipo_etiqueta}): {precio_base:.2f}. Rango aceptado: ({precio_min_logico:.2f} - {precio_max_logico:.2f})</i>")

        except Exception as e:
            print(f"  <i>- Error al calcular precio base para {tipo_etiqueta}: {e}. Saltando...</i>")
            continue
            
        datos_pagina_1 = procesar_datos(raw_page_1, fecha_hora_actual, tipo_etiqueta, precio_min_logico, precio_max_logico)
        todos_los_datos_tipo.extend(datos_pagina_1)

        for pagina in range(2, num_paginas + 1):
            raw_p2p_data = obtener_datos_p2p(pagina, trade_type)
            if raw_p2p_data:
                datos_pagina = procesar_datos(raw_p2p_data, fecha_hora_actual, tipo_etiqueta, precio_min_logico, precio_max_logico)
                todos_los_datos_tipo.extend(datos_pagina)
        
        datos_por_tipo[tipo_etiqueta] = pd.DataFrame(todos_los_datos_tipo)
        print(f"  <i>- Anuncios de {tipo_etiqueta} recolectados: {len(datos_por_tipo[tipo_etiqueta])}</i>")

    df_demanda_nuevo = datos_por_tipo.get("Demanda", pd.DataFrame())
    df_oferta_nuevo = datos_por_tipo.get("Oferta", pd.DataFrame())

    df_para_guardar = pd.concat([df_demanda_nuevo, df_oferta_nuevo], ignore_index=True)

    registros_anadidos = 0 

    if not df_para_guardar.empty:
        try:
            guardar_en_bd(df_para_guardar)
            registros_anadidos = len(df_para_guardar) 
            print(f"\n📊 ¡Éxito! {registros_anadidos} nuevos registros añadidos a la BD.")
            
        except Exception as e:
            # Relanzamos el error para que el bucle principal lo capture
            raise Exception(f"\n❌ ERROR al guardar en la base de datos: {e}")
            
    else:
        print("No se obtuvieron datos válidos en este ciclo. La base de datos no fue modificada.")
    
    return registros_anadidos

# --- 6. BUCLE PRINCIPAL (24/7) ---
if __name__ == "__main__":
    
    if engine is None:
        print("Finalizando script. No hay conexión a la base de datos.")
    else:
        total_registros_acumulados = 0
        tiempo_espera_segundos = 120 # 2 minutos
        
        print(f"\n--- Programación iniciada. El script se ejecutará cada {tiempo_espera_segundos} segundos. ---")
        print(f"--- Usando FILTRO DINÁMICO con tolerancia de {TOLERANCIA_PRECIO*100}% ---")
        print("--- Presiona Ctrl+C para detener. ---")

        while True:
            try:
                registros_este_ciclo = ejecutar_extraccion(num_paginas=10)
                total_registros_acumulados += registros_este_ciclo
                
                print(f"\n⭐ Total acumulado en esta sesión: {total_registros_acumulados} registros.")
                print("-" * 50)
                
            except KeyboardInterrupt:
                print("\n--- Script detenido manualmente por el usuario. ---")
                break
                
            except Exception as e:
                print("\n" + "="*50)
                print(f"🔥 ERROR INESPERADO EN EL CICLO PRINCIPAL 🔥")
                print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print("El script NO se detendrá. Reintentando en el próximo ciclo.")
                print("\nDetalle del error:")
                traceback.print_exc() 
                print("="*50 + "\n")
            
            print(f"--- Siguiente ciclo en {tiempo_espera_segundos} segundos... ---")
            time.sleep(tiempo_espera_segundos)