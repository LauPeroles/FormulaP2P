import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
# --- IMPORTACIONES CORREGIDAS ---
from dash import Dash, html, dcc, callback_context, Input, Output, State
import datetime
from dash.exceptions import PreventUpdate 
# --- LÍNEA CORREGIDA (de 'sqlalchzemy') ---
from sqlalchemy import create_engine 
import os
from dateutil.relativedelta import relativedelta # Esta línea necesita 'python-dateutil'

# --- CONFIGURACIÓN DE BASE DE DATOS ---
TABLE_NAME = 'p2p_anuncios'
DATABASE_URL = os.environ.get("DATABASE_URL")

# Forzar prefijo 'postgresql://'
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    
try:
    ENGINE = create_engine(DATABASE_URL)
    print(f"[{datetime.datetime.now()}] Conexión a PostgreSQL establecida.")
except Exception as e:
    print(f"[{datetime.datetime.now()}] ERROR FATAL: No se pudo crear engine de SQLAlchemy: {e}")
    ENGINE = None

# --- CONSTANTES DE COLOR ---
COLOR_BACKGROUND_APP = '#0d0d0d'
COLOR_CARD_BACKGROUND = '#1a1a1a'
COLOR_BORDER = '#333333'
COLOR_TEXT = '#f0f0f0'
COLOR_HIGHLIGHT = '#00CC96'
COLOR_PRECIO_VENTA = '#E74C3C' 
COLOR_PRECIO_COMPRA = '#2ECC71'
COLOR_VOL_VENTA = '#C0392B'
COLOR_VOL_COMPRA = '#27AE60'
COLOR_VOL_TOTAL = '#3498DB' 
COLOR_SPREAD = 'rgba(255, 255, 255, 0.1)'
PALETA_METODOS = [
    '#3498DB', '#E67E22', '#2ECC71', '#9B59B6', '#F1C40F', 
    '#1ABC9C', '#D35400', '#2C3E50', '#BDC3C7', '#7F8C8D'
]
DEFAULT_TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M'

# --- CORRECCIÓN DE WARNINGS DE PANDAS ('H' -> 'h') ---
DEFAULT_INTERVAL = '1h' # '1H' está obsoleto
DEFAULT_CHART_TYPE = 'tab-velas'

# --- AJUSTE FINAL DE RAM ---
# Límite de horas para la carga de datos.
# Bajamos de 12 a 6 horas para el intento final.
HOURS_TO_LOAD = 6

# --- DEFINICIÓN DE ESTILOS CSS ---
EXTERNAL_STYLESHEET = [
    'https://fonts.googleapis.com/css2?family=Roboto:wght@300;400;700&display=swap'
]
APP_CSS = f"""
    body {{
        font-family: 'Roboto', sans-serif;
        background-color: {COLOR_BACKGROUND_APP};
        color: {COLOR_TEXT};
        margin: 0; padding: 0;
    }}
    .container {{ max-width: 1400px; margin: 0 auto; padding: 20px; }}
    /* 'app-title' es el ID de nuestro H1, para poder actualizarlo */
    h1#app-title {{
        font-weight: 700; font-size: 2.5em; margin-bottom: 5px;
        color: #FFFFFF; text-align: center; letter-spacing: 1px;
    }}
    .Tabs {{
        background-color: {COLOR_CARD_BACKGROUND};
        border-radius: 8px; overflow: hidden;
        border: 1px solid {COLOR_BORDER};
        margin-bottom: 20px; display: flex;
    }}
    .Tab {{
        background-color: {COLOR_CARD_BACKGROUND};
        color: rgba(255,255,255,0.7);
        padding: 12px 20px;
        border: none;
        cursor: pointer;
        font-size: 1.1em;
        transition: all 0.2s ease;
        border-bottom: 3px solid transparent;
        border-right: 1px solid {COLOR_BORDER};
        text-transform: uppercase;
        font-weight: 500;
        letter-spacing: 0.5px;
    }}
    .Tab:hover {{ background-color: #272727; color: {COLOR_HIGHLIGHT}; }}
    .Tab:last-child {{ border-right: none; }}
    .Tab--selected {{
        background-color: {COLOR_HIGHLIGHT} !important;
        color: {COLOR_BACKGROUND_APP} !important;
        border-bottom: 3px solid {COLOR_HIGHLIGHT} !important;
        font-weight: 700;
        box-shadow: inset 0 2px 4px rgba(0, 0, 0, 0.4);
    }}
    #output-rango-fecha {{
        border: 1px solid {COLOR_BORDER};
        background-color: {COLOR_CARD_BACKGROUND};
        padding: 12px 0; font-size: 1.15em; letter-spacing: 0.7px;
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.4);
        border-radius: 8px; margin-bottom: 25px; font-weight: 400;
        text-align: center; width: 95%; margin-left: auto; margin-right: auto;
        position: relative; z-index: 10; margin-top: -35px;
    }}
    details {{
        background-color: {COLOR_CARD_BACKGROUND};
        border: 1px solid {COLOR_BORDER};
        border-radius: 8px; margin-bottom: 15px;
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.4);
        transition: all 0.3s ease-in-out;
    }}
    details:hover {{ border-color: {COLOR_HIGHLIGHT}; box-shadow: 0 0 15px rgba(0, 204, 150, 0.2); }}
    summary {{
        padding: 15px 20px; cursor: pointer; outline: none;
        list-style: none; font-size: 1.3em; font-weight: 700;
        color: {COLOR_TEXT}; position: relative;
        display: flex; align-items: center;
    }}
    summary::before {{
        content: '▶'; font-size: 0.7em; margin-right: 10px;
        color: {COLOR_HIGHLIGHT}; transition: transform 0.2s;
    }}
    details[open] > summary::before {{ content: '▼'; }}
    .graph-container {{ padding: 15px; border-top: 1px solid {COLOR_BORDER}; }}
    .graph-separator {{ border-bottom: 1px dashed {COLOR_BORDER}; margin: 20px 0; }}
    .interval-selector {{
        display: flex; justify-content: center; margin-bottom: 20px;
        background-color: {COLOR_CARD_BACKGROUND};
        padding: 8px; border-radius: 8px;
        border: 1px solid {COLOR_BORDER};
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.4);
    }}
    .radio-item {{ margin: 0 12px; cursor: pointer; }}
    .radio-item input[type="radio"] {{
        -webkit-appearance: none; appearance: none;
        width: 14px; height: 14px;
        border: 2px solid {COLOR_BORDER};
        border-radius: 50%; margin-right: 5px;
        transition: border-color 0.2s;
        vertical-align: middle; position: relative; top: -1px;
    }}
    .radio-item input[type="radio"]:checked {{
        border: 2px solid {COLOR_HIGHLIGHT};
        background-color: {COLOR_HIGHLIGHT};
        box-shadow: 0 0 0 2px {COLOR_BACKGROUND_APP};
    }}
    .radio-item label {{ color: rgba(255,255,255,0.8); font-weight: 400; }}
"""

# --- 1. PROCESAMIENTO DE DATOS (¡OPTIMIZADO!) ---

def cargar_datos_crudos(hours_to_load=HOURS_TO_LOAD):
    """
    Carga datos desde PostgreSQL, limitando el histórico para ahorrar RAM.
    """
    if ENGINE is None:
        print(f"[{datetime.datetime.now()}] cargar_datos_crudos abortado: No hay conexión a DB.")
        return pd.DataFrame(), pd.DataFrame(), "P2P (Error)"

    try:
        # --- CAMBIO A 6 HORAS ---
        start_date = datetime.datetime.now() - relativedelta(hours=hours_to_load) 
        start_date_str = start_date.strftime("%Y-%m-%d %H:%M:%S")

        exchange_name = "P2P"
        df_raw = pd.DataFrame()
        
        try:
            sql_query = f"""
            SELECT "Timestamp", "Tipo", "Precio", "Volumen", "Metodos_Pago", "Exchange_Name"
            FROM {TABLE_NAME}
            WHERE "Timestamp" >= '{start_date_str}'
            ORDER BY "Timestamp"
            """
            print(f"[{datetime.datetime.now()}] Cargando datos (ÚLTIMAS {hours_to_load} HORAS): Desde {start_date_str}...")
            df_raw = pd.read_sql(sql_query, con=ENGINE)
            if not df_raw.empty and 'Exchange_Name' in df_raw.columns and not df_raw['Exchange_Name'].empty:
                 first_valid_name = df_raw['Exchange_Name'].dropna().iloc[0]
                 if first_valid_name:
                     exchange_name = first_valid_name
                 else:
                     exchange_name = "P2P (Nombre no disp.)"

        except Exception as e_col:
            # Esto se ejecuta si la DB fue creada por el script de "reparación" (fix_db.py)
            # que borró la tabla y el scraper aún no ha guardado la columna Exchange_Name.
            print(f"[{datetime.datetime.now()}] Advertencia: Columna 'Exchange_Name' no encontrada. Reintentando sin ella. {e_col}")
            sql_query_fallback = f"""
            SELECT "Timestamp", "Tipo", "Precio", "Volumen", "Metodos_Pago"
            FROM {TABLE_NAME}
            WHERE "Timestamp" >= '{start_date_str}'
            ORDER BY "Timestamp"
            """
            df_raw = pd.read_sql(sql_query_fallback, con=ENGINE)
            exchange_name = "P2P (Fallback)" 

        
        if df_raw.empty:
            print(f"[{datetime.datetime.now()}] No hay datos recientes en el rango.")
            return pd.DataFrame(), pd.DataFrame(), exchange_name
            
        print(f"[{datetime.datetime.now()}] ✅ Cargados {len(df_raw)} registros recientes.")

        # Convertir tipos
        df_raw['Timestamp'] = pd.to_datetime(df_raw['Timestamp'])
        df_raw['Precio'] = pd.to_numeric(df_raw['Precio'], errors='coerce')
        df_raw['Volumen'] = pd.to_numeric(df_raw['Volumen'], errors='coerce')
        df_raw.dropna(subset=['Precio', 'Volumen'], inplace=True) 

        # Procesar métodos de pago
        # Usamos .copy() para evitar SettingWithCopyWarning
        df_metodos = df_raw.copy() 
        df_metodos['Metodos_Pago'] = df_metodos['Metodos_Pago'].fillna('')
        df_metodos['Metodos_Pago'] = df_metodos['Metodos_Pago'].str.split(r',\s*')
        
        df_metodos_expl = df_metodos.explode('Metodos_Pago')
        df_metodos_expl['Metodos_Pago'] = df_metodos_expl['Metodos_Pago'].str.strip()
        df_metodos_expl['Metodos_Pago'] = df_metodos_expl['Metodos_Pago'].replace('', 'Indefinido')
        
        return df_raw, df_metodos_expl, exchange_name

    except Exception as e:
        print(f"[{datetime.datetime.now()}] ❌ ERROR de DB en cargar_datos_crudos: {e}")
        return pd.DataFrame(), pd.DataFrame(), "P2P (Error)"

def crear_datos_ohlc(df_raw, interval):
    if df_raw.empty: return pd.DataFrame(), pd.DataFrame()
    df_raw_indexed = df_raw.set_index('Timestamp')
    ohlcv_agg = {'Precio': 'ohlc', 'Volumen': 'sum'}
    
    # El 'interval' (ej. '1h') viene del RadioItems, ya está en minúsculas
    df_demanda = df_raw_indexed[df_raw_indexed['Tipo'] == 'Demanda'].resample(interval).agg(ohlcv_agg).dropna()
    df_demanda.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    df_oferta = df_raw_indexed[df_raw_indexed['Tipo'] == 'Oferta'].resample(interval).agg(ohlcv_agg).dropna()
    df_oferta.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    return df_demanda, df_oferta

# --- 2. CREACIÓN DE GRÁFICOS (Funciones de Visualización) ---

def _crear_grafico_vacio(mensaje="Cargando datos..."):
    fig = go.Figure()
    fig.add_annotation(text=mensaje, xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False, font=dict(size=16, color=COLOR_TEXT))
    fig.update_layout(height=350, template="plotly_dark", plot_bgcolor=COLOR_CARD_BACKGROUND, paper_bgcolor=COLOR_CARD_BACKGROUND, xaxis=dict(showgrid=False, zeroline=False, showticklabels=False), yaxis=dict(showgrid=False, zeroline=False, showticklabels=False))
    return fig

# --- VISTA 1: Estilo Trading (Velas) ---
def crear_figura_velas(df_demanda_ohlc, df_oferta_ohlc, interval):
    if df_demanda_ohlc.empty and df_oferta_ohlc.empty:
        return _crear_grafico_vacio(f"No hay datos para el intervalo {interval}")
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.03, row_heights=[0.8, 0.2])
    fig.add_trace(go.Candlestick(x=df_demanda_ohlc.index, open=df_demanda_ohlc['Open'], high=df_demanda_ohlc['High'], low=df_demanda_ohlc['Low'], close=df_demanda_ohlc['Close'], name='Demanda (Compra)', increasing_line_color=COLOR_PRECIO_COMPRA, decreasing_line_color=COLOR_PRECIO_COMPRA, line=dict(width=1.5)), row=1, col=1)
    fig.add_trace(go.Candlestick(x=df_oferta_ohlc.index, open=df_oferta_ohlc['Open'], high=df_oferta_ohlc['High'], low=df_oferta_ohlc['Low'], close=df_oferta_ohlc['Close'], name='Oferta (Venta)', increasing_line_color=COLOR_PRECIO_VENTA, decreasing_line_color=COLOR_PRECIO_VENTA, line=dict(width=1.5)), row=1, col=1)
    fig.add_trace(go.Bar(x=df_demanda_ohlc.index, y=df_demanda_ohlc['Volume'], name='Vol. Compra', marker_color=COLOR_VOL_COMPRA, showlegend=False), row=2, col=1)
    fig.add_trace(go.Bar(x=df_oferta_ohlc.index, y=df_oferta_ohlc['Volume'], name='Vol. Venta', marker_color=COLOR_VOL_VENTA, showlegend=False), row=2, col=1)
    fig.update_layout(height=600, template="plotly_dark", hovermode="x unified", title={'text': f'Estilo Trading (Intervalo: {interval})'}, legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5), xaxis_rangeslider_visible=False, plot_bgcolor=COLOR_CARD_BACKGROUND, paper_bgcolor=COLOR_BACKGROUND_APP, barmode='overlay')
    fig.update_yaxes(title_text="Precio USDT (VES)", row=1, col=1, gridcolor='rgba(255,255,255,0.08)')
    fig.update_yaxes(title_text="Volumen USDT", row=2, col=1, showgrid=False)
    fig.update_xaxes(gridcolor='rgba(255,255,255,0.08)', row=2, col=1)
    fig.update_xaxes(showticklabels=False, row=1, col=1)
    return fig

# --- VISTA 2: Estilo Analítico (Área de Spread) ---
def crear_figura_spread(df_demanda_ohlc, df_oferta_ohlc, interval):
    if df_demanda_ohlc.empty and df_oferta_ohlc.empty:
        return _crear_grafico_vacio(f"No hay datos para el intervalo {interval}")
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.03, row_heights=[0.8, 0.2])
    df_combinado = pd.merge(df_demanda_ohlc[['Close', 'Volume']], df_oferta_ohlc[['Close', 'Volume']], left_index=True, right_index=True, how='outer', suffixes=('_D', '_O'))
    df_combinado['Close_D'] = df_combinado['Close_D'].ffill()
    df_combinado['Close_O'] = df_combinado['Close_O'].ffill()
    df_combinado['Volumen_Total'] = df_combinado['Volume_D'].fillna(0) + df_combinado['Volume_O'].fillna(0)
    fig.add_trace(go.Scatter(x=df_combinado.index, y=df_combinado['Close_D'], mode='lines', line=dict(color=COLOR_PRECIO_COMPRA, width=1.5), name='Demanda (Compra)', hovertemplate='Compra: <b>%{y:.2f} VES</b><extra></extra>'), row=1, col=1)
    fig.add_trace(go.Scatter(x=df_combinado.index, y=df_combinado['Close_O'], mode='lines', line=dict(color=COLOR_PRECIO_VENTA, width=1.5), fill='tonexty', fillcolor=COLOR_SPREAD, name='Oferta (Venta)', hovertemplate='Venta: <b>%{y:.2f} VES</b><extra></extra>'), row=1, col=1)
    fig.add_trace(go.Bar(x=df_combinado.index, y=df_combinado['Volumen_Total'], name='Volumen Total', marker_color=COLOR_VOL_TOTAL, showlegend=False), row=2, col=1)
    fig.update_layout(height=600, template="plotly_dark", hovermode="x unified", title={'text': f'Estilo Analítico (Intervalo: {interval})'}, legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5), xaxis_rangeslider_visible=False, plot_bgcolor=COLOR_CARD_BACKGROUND, paper_bgcolor=COLOR_BACKGROUND_APP)
    fig.update_yaxes(title_text="Precio USDT (VES)", row=1, col=1, gridcolor='rgba(255,255,255,0.08)')
    fig.update_yaxes(title_text="Volumen USDT", row=2, col=1, showgrid=False)
    fig.update_xaxes(gridcolor='rgba(255,255,255,0.08)', row=2, col=1)
    fig.update_xaxes(showticklabels=False, row=1, col=1)
    return fig

# --- VISTA 3: Estilo Moderno (Línea/Burbuja) ---
def crear_figura_burbuja(df_demanda_ohlc, df_oferta_ohlc, interval):
    if df_demanda_ohlc.empty and df_oferta_ohlc.empty:
        return _crear_grafico_vacio(f"No hay datos para el intervalo {interval}")
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.03, row_heights=[0.8, 0.2])
    df_combinado = pd.merge(df_demanda_ohlc[['Close', 'Volume']], df_oferta_ohlc[['Close', 'Volume']], left_index=True, right_index=True, how='outer', suffixes=('_D', '_O'))
    df_combinado['Volumen_Total'] = df_combinado['Volume_D'].fillna(0) + df_combinado['Volume_O'].fillna(0)
    if not df_combinado['Volumen_Total'].empty and df_combinado['Volumen_Total'].max() > 0:
        max_vol = df_combinado['Volumen_Total'].max()
        df_combinado['Bubble_Size'] = df_combinado['Volumen_Total'].apply(lambda x: 5 + (x/max_vol) * 25)
    else: df_combinado['Bubble_Size'] = 5
    fig.add_trace(go.Scatter(x=df_demanda_ohlc.index, y=df_demanda_ohlc['Close'], mode='lines+markers', name='Demanda (Compra)', line=dict(color=COLOR_PRECIO_COMPRA, width=3, shape='spline'), marker=dict(size=df_combinado['Bubble_Size'], color=COLOR_PRECIO_COMPRA, line=dict(width=1, color=COLOR_CARD_BACKGROUND)), hovertemplate='Compra: <b>%{y:.2f} VES</b><br>Vol. Total: %{customdata:,.0f} USDT<extra></extra>', customdata=df_combinado['Volumen_Total']), row=1, col=1)
    fig.add_trace(go.Scatter(x=df_oferta_ohlc.index, y=df_oferta_ohlc['Close'], mode='lines+markers', name='Oferta (Venta)', line=dict(color=COLOR_PRECIO_VENTA, width=3, shape='spline', dash='dot'), marker=dict(size=df_combinado['Bubble_Size'], color=COLOR_PRECIO_VENTA, line=dict(width=1, color=COLOR_CARD_BACKGROUND)), hovertemplate='Venta: <b>%{y:.2f} VES</b><br>Vol. Total: %{customdata:,.0f} USDT<extra></extra>', customdata=df_combinado['Volumen_Total']), row=1, col=1)
    fig.add_trace(go.Bar(x=df_combinado.index, y=df_combinado['Volumen_Total'], name='Volumen Total', marker_color=COLOR_VOL_TOTAL, showlegend=False), row=2, col=1)
    fig.update_layout(height=600, template="plotly_dark", hovermode="x unified", title={'text': f'Estilo Moderno (Intervalo: {interval})'}, legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5), xaxis_rangeslider_visible=False, plot_bgcolor=COLOR_CARD_BACKGROUND, paper_bgcolor=COLOR_BACKGROUND_APP)
    fig.update_yaxes(title_text="Precio USDT (VES)", row=1, col=1, gridcolor='rgba(255,255,255,0.08)')
    fig.update_yaxes(title_text="Volumen USDT", row=2, col=1, showgrid=False)
    fig.update_xaxes(gridcolor='rgba(255,255,255,0.08)', row=2, col=1)
    fig.update_xaxes(showticklabels=False, row=1, col=1)
    return fig

# --- GRÁFICOS DE MÉTODOS (Funciones) ---
def crear_grafico_premium(df_metodos_expl, fecha_inicio, fecha_fin):
    if df_metodos_expl.empty: return _crear_grafico_vacio("No hay datos de métodos")
    df_filtrado_tiempo = df_metodos_expl[(df_metodos_expl['Timestamp'] >= fecha_inicio) & (df_metodos_expl['Timestamp'] <= fecha_fin)]
    if df_filtrado_tiempo.empty: return _crear_grafico_vacio("No hay datos de métodos de pago en este rango")
    top_10_metodos_por_volumen = df_filtrado_tiempo.groupby('Metodos_Pago')['Volumen'].sum().nlargest(10).index
    df_top_10 = df_filtrado_tiempo[df_filtrado_tiempo['Metodos_Pago'].isin(top_10_metodos_por_volumen)]
    df_precios_promedio = df_top_10.groupby(['Metodos_Pago', 'Tipo'])['Precio'].mean().reset_index()
    df_demanda = df_precios_promedio[df_precios_promedio['Tipo'] == 'Demanda'].sort_values('Precio', ascending=True)
    df_oferta = df_precios_promedio[df_precios_promedio['Tipo'] == 'Oferta']
    fig = go.Figure()
    fig.add_trace(go.Bar(y=df_demanda['Metodos_Pago'], x=df_demanda['Precio'], name='Precio Compra (Demanda)', orientation='h', marker_color=COLOR_PRECIO_COMPRA, hovertemplate='Compra: <b>%{x:.2f} VES</b><extra></extra>'))
    fig.add_trace(go.Bar(y=df_oferta['Metodos_Pago'], x=df_oferta['Precio'], name='Precio Venta (Oferta)', orientation='h', marker_color=COLOR_PRECIO_VENTA, hovertemplate='Venta: <b>%{x:.2f} VES</b><extra></extra>'))
    rango_titulo = f"{fecha_inicio.strftime('%b %d')} - {fecha_fin.strftime('%b %d, %H:%M')}"
    fig.update_layout(height=400, template="plotly_dark", barmode='group', title={'text': f'1. Premium: Precio Promedio por Método (Rango: {rango_titulo})', 'font': dict(size=18, color=COLOR_TEXT, family='Roboto')}, legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5), xaxis=dict(title='Precio Promedio (VES)', gridcolor='rgba(255,255,255,0.08)'), yaxis=dict(title='Métodos (Top 10 por Volumen)', showgrid=False, categoryorder='array', categoryarray=df_demanda['Metodos_Pago']), plot_bgcolor=COLOR_CARD_BACKGROUND, paper_bgcolor=COLOR_CARD_BACKGROUND, margin=dict(l=100))
    return fig

def crear_grafico_flujo(df_metodos_expl, fecha_inicio, fecha_fin):
    if df_metodos_expl.empty: return _crear_grafico_vacio("No hay datos de métodos")
    df_filtrado = df_metodos_expl[(df_metodos_expl['Timestamp'] >= fecha_inicio) & (df_metodos_expl['Timestamp'] <= fecha_fin)]
    if df_filtrado.empty: return _crear_grafico_vacio()
    top_10_metodos = df_filtrado.groupby('Metodos_Pago')['Volumen'].sum().nlargest(10).index
    df_top_10 = df_filtrado[df_filtrado['Metodos_Pago'].isin(top_10_metodos)]
    df_volumen = df_top_10.groupby(['Metodos_Pago', 'Tipo'])['Volumen'].sum().unstack(fill_value=0).reset_index()
    if 'Demanda' not in df_volumen: df_volumen['Demanda'] = 0
    if 'Oferta' not in df_volumen: df_volumen['Oferta'] = 0
    df_volumen['Total'] = df_volumen['Demanda'] + df_volumen['Oferta']
    df_volumen = df_volumen.sort_values('Total', ascending=True)
    fig = go.Figure()
    fig.add_trace(go.Bar(y=df_volumen['Metodos_Pago'], x=df_volumen['Demanda'], name='Vol. Compra (Demanda)', orientation='h', marker_color=COLOR_VOL_COMPRA, hovertemplate='Compra: <b>%{x:,.0f} USDT</b><extra></extra>'))
    fig.add_trace(go.Bar(y=df_volumen['Metodos_Pago'], x=df_volumen['Oferta'], name='Vol. Venta (Oferta)', orientation='h', marker_color=COLOR_VOL_VENTA, hovertemplate='Venta: <b>%{x:,.0f} USDT</b><extra></extra>'))
    fig.update_layout(height=400, template="plotly_dark", barmode='stack', title={'text': '2. Flujo: Volumen por Método (Oferta vs. Demanda)', 'font': dict(size=18, color=COLOR_TEXT, family='Roboto')}, xaxis=dict(title='Volumen Total (USDT)', gridcolor='rgba(255,255,255,0.08)'), yaxis=dict(title='Métodos (Top 10)', showgrid=False), plot_bgcolor=COLOR_CARD_BACKGROUND, paper_bgcolor=COLOR_CARD_BACKGROUND, legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5), margin=dict(l=100))
    return fig

def crear_grafico_tendencia(df_metodos_expl, fecha_inicio, fecha_fin):
    if df_metodos_expl.empty: return _crear_grafico_vacio("No hay datos de métodos")
    
    # --- CORRECCIÓN SETTINGWITHCOPYWARNING ---
    # Al filtrar, creamos una copia explícita con .copy()
    df_filtrado = df_metodos_expl[(df_metodos_expl['Timestamp'] >= fecha_inicio) & (df_metodos_expl['Timestamp'] <= fecha_fin)].copy()
    
    if df_filtrado.empty: return _crear_grafico_vacio()
    
    duration = fecha_fin - fecha_inicio
    duration_days = duration.total_seconds() / (24 * 60 * 60) 
    
    # --- CORRECCIÓN FUTUREWARNING ('H' -> 'h', 'D' -> 'd') ---
    if duration_days <= 2: interval, interval_label = '1h', "1 Hora"
    elif duration_days <= 14: interval, interval_label = '6h', "6 Horas"
    else: interval, interval_label = '1d', "1 Día"
        
    top_metodos = df_filtrado.groupby('Metodos_Pago')['Volumen'].sum().nlargest(7).index
    
    # Esta es la línea que causaba el warning (336 aprox). Ahora es segura gracias al .copy() de arriba.
    df_filtrado['Metodo_Agrupado'] = df_filtrado['Metodos_Pago'].apply(lambda x: x if x in top_metodos else 'Otros')
    
    df_resampled = (df_filtrado.set_index('Timestamp').groupby('Metodo_Agrupado').resample(interval)['Volumen'].sum().unstack(level=0, fill_value=0))
    if 'Otros' in df_resampled.columns: df_resampled = df_resampled[[col for col in df_resampled if col != 'Otros'] + ['Otros']]
    fig = go.Figure()
    for i, metodo in enumerate(df_resampled.columns):
        color = PALETA_METODOS[i % len(PALETA_METODOS)] if metodo != 'Otros' else '#7F8C8D'
        fig.add_trace(go.Scatter(x=df_resampled.index, y=df_resampled[metodo], name=metodo, mode='lines', line=dict(width=0.5, color=color), stackgroup='one', groupnorm='percent', hovertemplate=f'<b>{metodo}</b><br>%{{y:.1f}}%<extra></extra>'))
    fig.update_layout(height=400, template="plotly_dark", title={'text': f'3. Tendencia: Cuota de Mercado (Intervalo: {interval_label})', 'font': dict(size=18, color=COLOR_TEXT, family='Roboto')}, xaxis=dict(title='Fecha', gridcolor='rgba(255,255,255,0.08)'), yaxis=dict(title='Cuota de Mercado (%)', showgrid=False, ticksuffix='%'), plot_bgcolor=COLOR_CARD_BACKGROUND, paper_bgcolor=COLOR_CARD_BACKGROUND, legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5), hovermode='x unified', margin=dict(l=100))
    return fig


# --- 3. FUNCIONES AUXILIARES ---

def obtener_rango_fechas_del_grafico(relayout_data, df_ohlc_actual):
    if relayout_data is None or 'xaxis.range[0]' not in relayout_data:
        if not df_ohlc_actual.empty: return df_ohlc_actual.index.min(), df_ohlc_actual.index.max()
        else: return datetime.datetime.now(), datetime.datetime.now()
    try:
        fecha_inicio_str = relayout_data['xaxis.range[0]']
        fecha_fin_str = relayout_data['xaxis.range[1]']
        return pd.to_datetime(fecha_inicio_str), pd.to_datetime(fecha_fin_str)
    except Exception:
        return df_ohlc_actual.index.min(), df_ohlc_actual.index.max()

def crear_texto_rango_fechas(fecha_inicio, fecha_fin):
    return html.Span([
        html.Span("RANGO DE FECHA: ", style={'color': 'white', 'fontWeight': '400'}),
        html.Span(f"{fecha_inicio.strftime(DEFAULT_TIMESTAMP_FORMAT)}", style={'color': COLOR_PRECIO_COMPRA, 'fontWeight': '700'}),
        html.Span(" — ", style={'color': 'gray'}),
        html.Span(f"{fecha_fin.strftime(DEFAULT_TIMESTAMP_FORMAT)}", style={'color': COLOR_PRECIO_VENTA, 'fontWeight': '700'})
    ])

# --- 4. INICIALIZACIÓN DE DASH ---

app = Dash(__name__, external_stylesheets=EXTERNAL_STYLESHEET)
server = app.server # Variable server para Gunicorn

app.index_string = f'''
<!DOCTYPE html>
<html>
    <head>
        {{%metas%}}  
        <title>Dashboard P2P Modular</title>
        <style>{APP_CSS}</style>
    </head>
    <body>
        <div id="react-entry-point">
            {{%app_entry%}}
        </div>
        <footer>
            {{%config%}}
            {{%scripts%}}
            {{%renderer%}}
        </footer>
    </body>
</html>
'''

# --- 5. LAYOUT DE LA APLICACIÓN (ARQUITECTURA "ESQUELETO") ---
def crear_layout():
    print(f"[{datetime.datetime.now()}] Iniciando crear_layout() (Modo Esqueleto)...")

    if ENGINE is None:
        print(f"[{datetime.datetime.now()}] Layout: ERROR CRÍTICO, sin engine de BD.")
        return html.Div([
            html.H1("ERROR CRÍTICO: CONEXIÓN A DB", style={'textAlign': 'center', 'color': 'red'}),
            html.P(f"La variable DATABASE_URL es incorrecta o el servidor no pudo conectar. Revisa la configuración de Render.", style={'textAlign': 'center', 'color': 'white'})
        ], className='container')
    else:
        print(f"[{datetime.datetime.now()}] Layout: Renderizando esqueleto de la app...")
        app_title = html.H1(f"Análisis de Mercado P2P", id='app-title') 
        figura_vacia_principal = _crear_grafico_vacio("Cargando datos...")
        figura_vacia_avanzada = _crear_grafico_vacio("Cargando...")
        texto_fecha_inicial = html.Span("Cargando rango de fechas...")

        return html.Div([
            dcc.Store(id='store-raw-data'),
            dcc.Store(id='store-methods-data'),
            
            dcc.Interval(
                id='interval-data-refresh', 
                interval=15 * 60 * 1000, 
                n_intervals=0,
                max_intervals=-1 
            ),
            dcc.Interval(
                id='interval-initial-load',
                interval=1 * 1000, 
                n_intervals=0,
                max_intervals=1 
            ),
            
            app_title,
            
            html.Div(className='interval-selector', children=[
                dcc.RadioItems(
                    id='interval-selector',
                    # --- CORRECCIÓN FUTUREWARNING ('H' -> 'h', 'D' -> 'd') ---
                    options=[
                        {'label': '15 Minutos', 'value': '15t', 'className': 'radio-item'},
                        {'label': '1 Hora', 'value': '1h', 'className': 'radio-item'},
                        {'label': '4 Horas', 'value': '4h', 'className': 'radio-item'},
                        {'label': '1 Día', 'value': '1d', 'className': 'radio-item'},
                    ],
                    value=DEFAULT_INTERVAL,
                    labelStyle={'display': 'inline-block'},
                )
            ]),
            
            dcc.Tabs(id="tabs-grafico-principal", value=DEFAULT_CHART_TYPE, className='Tabs', children=[
                dcc.Tab(label='Estilo Trading (Velas)', value='tab-velas', className='Tab', selected_className='Tab--selected'),
                dcc.Tab(label='Estilo Analítico (Spread)', value='tab-spread', className='Tab', selected_className='Tab--selected'),
                dcc.Tab(label='Estilo Moderno (Burbuja)', value='tab-burbuja', className='Tab', selected_className='Tab--selected'),
            ]),
            
            dcc.Graph(
                id='grafico-principal', 
                figure=figura_vacia_principal,
                config={'scrollZoom': True}
            ),
            
            html.Div(
                id='output-rango-fecha', 
                children=texto_fecha_inicial,
            ),
            
            html.Details(
                open=False, 
                children=[
                    html.Summary(
                        html.B("💳 Análisis Avanzado de Métodos de Pago"),
                    ),
                    html.Div(className='graph-container', children=[
                        dcc.Graph(id='grafico-metodos-premium', figure=figura_vacia_avanzada, config={'scrollZoom': True}),
                        html.Hr(className='graph-separator'),
                        dcc.Graph(id='grafico-metodos-flujo', figure=figura_vacia_avanzada, config={'scrollZoom': True}),
                        html.Hr(className='graph-separator'),
                        dcc.Graph(id='grafico-metodos-tendencia', figure=figura_vacia_avanzada, config={'scrollZoom': True})
                    ])
                ]
            ),
            
        ], className='container') 

print(f"[{datetime.datetime.now()}] Asignando crear_layout a app.layout...")
app.layout = crear_layout
print(f"[{datetime.datetime.now()}] Asignación de layout completada.")

# --- 6. CALLBACKS (¡OPTIMIZADOS CON DCC.STORE!) ---

# --- CALLBACK 1: Carga de Datos (Disparado por los Intervals) ---
@app.callback(
    Output('store-raw-data', 'data'),
    Output('store-methods-data', 'data'),
    Output('app-title', 'children'),
    [Input('interval-initial-load', 'n_intervals'),
     Input('interval-data-refresh', 'n_intervals')]
)
def update_global_data_store(n_initial, n_refresh):
    ctx = callback_context
    if not ctx.triggered:
        raise PreventUpdate
    
    trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]
    print(f"[{datetime.datetime.now()}] CALLBACK 1: Actualizando store (Disparado por: {trigger_id})...")
    
    # --- CAMBIO A 6 HORAS ---
    df_raw, df_metodos_expl, exchange_name = cargar_datos_crudos(hours_to_load=HOURS_TO_LOAD)
    
    titulo = f"Análisis de Mercado P2P: {exchange_name}"

    if df_raw.empty:
        print(f"[{datetime.datetime.now()}] CALLBACK 1: No se cargaron datos, no se actualiza el store.")
        titulo = f"Análisis de Mercado P2P: {exchange_name} (Sin datos recientes)"
        if trigger_id == 'interval-initial-load':
             return None, None, titulo
        raise PreventUpdate
    
    json_raw = df_raw.to_json(orient='split', date_format='iso')
    json_methods = df_metodos_expl.to_json(orient='split', date_format='iso')
    
    print(f"[{datetime.datetime.now()}] CALLBACK 1: Store de datos actualizado con {len(df_raw)} registros.")
    return json_raw, json_methods, titulo


# --- CALLBACK 2: Actualización de Gráficos (Disparado por Stores y Clics) ---
@app.callback(
    Output('grafico-principal', 'figure'),
    Output('grafico-metodos-premium', 'figure'),
    Output('grafico-metodos-flujo', 'figure'),
    Output('grafico-metodos-tendencia', 'figure'),
    Output('output-rango-fecha', 'children'),
    Input('store-raw-data', 'data'),
    Input('store-methods-data', 'data'),
    Input('tabs-grafico-principal', 'value'),
    Input('interval-selector', 'value'),
    Input('grafico-principal', 'relayoutData')
)
def actualizar_graficos(json_raw, json_methods, tab_value, interval_value, relayout_data):
    if not json_raw or not json_methods:
        print(f"[{datetime.datetime.now()}] CALLBACK 2: Esperando datos del store...")
        fig_vacia = _crear_grafico_vacio("Cargando datos...")
        texto_vacio = html.Span("Cargando...")
        return fig_vacia, fig_vacia, fig_vacia, fig_vacia, texto_vacio

    print(f"[{datetime.datetime.now()}] CALLBACK 2: Actualizando gráficos...")
    df_raw_global = pd.read_json(json_raw, orient='split')
    df_metodos_expl_global = pd.read_json(json_methods, orient='split')
    
    df_raw_global['Timestamp'] = pd.to_datetime(df_raw_global['Timestamp'], errors='coerce')
    df_metodos_expl_global['Timestamp'] = pd.to_datetime(df_metodos_expl_global['Timestamp'], errors='coerce')

    
    if df_raw_global.empty:
        return (_crear_grafico_vacio("No hay datos recientes"),) * 4 + (html.Span("Esperando datos..."),)

    ctx = callback_context
    trigger_id = ctx.triggered
    trigger_id_prop = trigger_id[0]['prop_id'].split('.')[0] if trigger_id else None
    
    df_demanda_ohlc, df_oferta_ohlc = crear_datos_ohlc(df_raw_global, interval_value)

    if trigger_id_prop == 'grafico-principal' and 'xaxis.range[0]' in (relayout_data or {}):
        fecha_inicio, fecha_fin = obtener_rango_fechas_del_grafico(relayout_data, df_demanda_ohlc)
    else:
        if df_demanda_ohlc.empty and df_oferta_ohlc.empty: 
             return (_crear_grafico_vacio(f"No hay datos para el intervalo {interval_value}"),) * 4 + (html.Span("Datos insuficientes..."),)
        
        min_d = df_demanda_ohlc.index.min() if not df_demanda_ohlc.empty else pd.Timestamp.max
        min_o = df_oferta_ohlc.index.min() if not df_oferta_ohlc.empty else pd.Timestamp.min
        max_d = df_demanda_ohlc.index.max() if not df_demanda_ohlc.empty else pd.Timestamp.min
        max_o = df_oferta_ohlc.index.max() if not df_oferta_ohlc.empty else pd.Timestamp.max
        
        fecha_inicio = min(min_d, min_o)
        fecha_fin = max(max_d, max_o)
        
        if fecha_inicio >= fecha_fin: 
            if fecha_inicio == pd.Timestamp.max:
                return (_crear_grafico_vacio(f"No hay datos para el intervalo {interval_value}"),) * 4 + (html.Span("Datos insuficientes..."),)
            fecha_fin = fecha_inicio + datetime.timedelta(hours=1)

    if trigger_id_prop == 'grafico-principal' and 'xaxis.range[0]' in (relayout_data or {}):
        fig_principal = PreventUpdate
    else:
        if (df_demanda_ohlc.empty and df_oferta_ohlc.empty):
             fig_principal = _crear_grafico_vacio(f"No hay datos para el intervalo {interval_value}")
        elif tab_value == 'tab-velas':
            fig_principal = crear_figura_velas(df_demanda_ohlc, df_oferta_ohlc, interval_value)
        elif tab_value == 'tab-spread':
            fig_principal = crear_figura_spread(df_demanda_ohlc, df_oferta_ohlc, interval_value)
        elif tab_value == 'tab-burbuja':
            fig_principal = crear_figura_burbuja(df_demanda_ohlc, df_oferta_ohlc, interval_value)
        else:
            fig_principal = crear_figura_velas(df_demanda_ohlc, df_oferta_ohlc, interval_value)

    fig_premium = crear_grafico_premium(df_metodos_expl_global, fecha_inicio, fecha_fin)
    fig_flujo = crear_grafico_flujo(df_metodos_expl_global, fecha_inicio, fecha_fin)
    fig_tendencia = crear_grafico_tendencia(df_metodos_expl_global, fecha_inicio, fecha_fin)
    
    texto_fecha = crear_texto_rango_fechas(fecha_inicio, fecha_fin)
    
    return fig_principal, fig_premium, fig_flujo, fig_tendencia, texto_fecha

# --- 7. EJECUCIÓN ---
if __name__ == '__main__':
    if ENGINE:
        print(f"[{datetime.datetime.now()}] Iniciando servidor de prueba local en http://127.0.0.1:8050")
        app.run_server(debug=True, host='0.0.0.0', port=8050)
    else:
        print(f"[{datetime.datetime.now()}] No se pudo iniciar el servidor. Revisa la conexión a la base de datos.")
