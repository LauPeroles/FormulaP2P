import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from dash import Dash, html, dcc, callback_context
from dash.dependencies import Input, Output, State
import datetime
import dash.exceptions 
from sqlalchemy import create_engine 
import os 

# --- CONFIGURACIÓN DE BASE DE DATOS ---
try:
    DATABASE_URL = os.environ.get('DATABASE_URL')
    if not DATABASE_URL:
        # Esto solo es para pruebas locales, en Render debe venir de la variable de entorno
        raise ValueError("No se encontró la variable de entorno DATABASE_URL")
    
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

    ENGINE = create_engine(DATABASE_URL)
    TABLE_NAME = 'p2p_anuncios'
except Exception as e:
    print(f"Error al crear engine de SQLAlchemy: {e}")
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
DEFAULT_INTERVAL = '1H'
DEFAULT_CHART_TYPE = 'tab-velas'

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
    .container {{
        max-width: 1400px;
        margin: 0 auto;
        padding: 20px;
    }}
    h1 {{
        font-weight: 700; font-size: 2.5em; margin-bottom: 20px;
        color: #FFFFFF; text-align: center; letter-spacing: 1px;
        text-shadow: 0 2px 4px rgba(0,0,0,0.3);
    }}
    
    /* --- Estilos para las Pestañas (Tabs) --- */
    .tabs-container {{
        background-color: {COLOR_CARD_BACKGROUND};
        border-radius: 8px;
        overflow: hidden;
        border: 1px solid {COLOR_BORDER};
        margin-bottom: 20px;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.4);
    }}
    .custom-tab {{
        background-color: {COLOR_CARD_BACKGROUND};
        color: rgba(255,255,255,0.7);
        padding: 12px 20px;
        border: none;
        cursor: pointer;
        font-size: 1.1em;
        transition: all 0.2s ease;
        border-bottom: 3px solid transparent;
        border-right: 1px solid {COLOR_BORDER};
    }}
    .custom-tab:last-child {{
        border-right: none;
    }}
    .custom-tab--selected {{
        background-color: {COLOR_CARD_BACKGROUND};
        color: {COLOR_HIGHLIGHT};
        border-bottom: 3px solid {COLOR_HIGHLIGHT};
        font-weight: 700;
    }}
    
    /* --- Estilos para los Selectores (RadioItems) --- */
    .interval-selector {{
        display: flex;
        justify-content: center;
        margin-bottom: 20px;
        background-color: {COLOR_CARD_BACKGROUND};
        padding: 10px 8px;
        border: 1px solid {COLOR_BORDER};
        border-radius: 8px;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.4);
    }}
    .interval-selector input[type="radio"] {{
        -webkit-appearance: none;
        -moz-appearance: none;
        appearance: none;
        width: 18px;
        height: 18px;
        border: 2px solid rgba(255,255,255,0.7);
        border-radius: 50%;
        outline: none;
        cursor: pointer;
        margin-right: 5px;
        position: relative;
        top: 4px;
        transition: all 0.2s ease;
    }}
    .interval-selector input[type="radio"]:checked {{
        border-color: {COLOR_HIGHLIGHT};
    }}
    .interval-selector input[type="radio"]:checked::before {{
        content: '';
        display: block;
        width: 10px;
        height: 10px;
        background-color: {COLOR_HIGHLIGHT};
        border-radius: 50%;
        position: absolute;
        top: 50%;
        left: 50%;
        transform: translate(-50%, -50%);
    }}
    .interval-selector label {{
        padding-right: 15px;
    }}

    /* --- Estilos para Contenedores y Gráficos --- */
    #grafico-principal {{
        border-radius: 8px;
        overflow: hidden;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.4);
        border: 1px solid {COLOR_BORDER};
    }}
    #output-rango-fecha {{
        border: 1px solid {COLOR_BORDER};
        background-color: {COLOR_CARD_BACKGROUND};
        padding: 12px 0; font-size: 1.15em; letter-spacing: 0.7px;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.4);
        margin-bottom: 25px; font-weight: 400;
        text-align: center; 
        width: 95%; margin-left: auto; margin-right: auto;
        position: relative; z-index: 10;
        margin-top: -35px;
        border-radius: 8px;
    }}
    details {{
        background-color: {COLOR_CARD_BACKGROUND};
        border: 1px solid {COLOR_BORDER};
        border-radius: 8px; margin-bottom: 15px;
        transition: all 0.3s ease-in-out;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.4);
    }}
    details:hover {{
        border-color: {COLOR_HIGHLIGHT};
    }}
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
    details[open] > summary::before {{
        content: '▼';
    }}
    .graph-container {{
        padding: 15px;
        border-top: 1px solid {COLOR_BORDER};
    }}
    .graph-separator {{
        border-bottom: 1px dashed {COLOR_BORDER};
        margin: 20px 0;
    }}
"""

# --- 1. PROCESAMIENTO DE DATOS ---

def cargar_datos_crudos():
    global ENGINE, TABLE_NAME
    if ENGINE is None:
        return pd.DataFrame(), pd.DataFrame()

    df_raw = pd.DataFrame()
    df_metodos_expl = pd.DataFrame()

    try:
        # Usamos read_sql_query en lugar de read_sql_table para manejar posibles errores de tabla/schema
        df_raw = pd.read_sql_query(f'SELECT * FROM {TABLE_NAME}', con=ENGINE)
        
        if df_raw.empty:
            raise Exception("La base de datos está vacía.")
            
        df_raw['Timestamp'] = pd.to_datetime(df_raw['Timestamp'])
        df_raw['Precio'] = pd.to_numeric(df_raw['Precio'], errors='coerce')
        df_raw['Volumen'] = pd.to_numeric(df_raw['Volumen'], errors='coerce')
        df_raw.dropna(subset=['Precio', 'Volumen'], inplace=True) 

        df_metodos = df_raw.copy()
        df_metodos['Metodos_Pago'] = df_metodos['Metodos_Pago'].fillna('')
        df_metodos['Metodos_Pago'] = df_metodos['Metodos_Pago'].str.split(r',\s*')
        
        df_metodos_expl = df_metodos.explode('Metodos_Pago')
        df_metodos_expl['Metodos_Pago'] = df_metodos_expl['Metodos_Pago'].str.strip()
        df_metodos_expl['Metodos_Pago'] = df_metodos_expl['Metodos_Pago'].replace('', 'Indefinido')
        
        return df_raw, df_metodos_expl

    except Exception as e:
        print(f"❌ Error al cargar datos de la BD: {e}")
        return pd.DataFrame(), pd.DataFrame()

# --- 2. CREACIÓN DE GRÁFICOS (Funciones simplificadas) ---

def crear_datos_ohlc(df_raw, interval):
    if df_raw.empty: return pd.DataFrame(), pd.DataFrame()
    df_raw_indexed = df_raw.set_index('Timestamp')
    ohlcv_agg = {'Precio': 'ohlc', 'Volumen': 'sum'}
    df_demanda = df_raw_indexed[df_raw_indexed['Tipo'] == 'Demanda'].resample(interval).agg(ohlcv_agg).dropna()
    df_demanda.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    df_oferta = df_raw_indexed[df_raw_indexed['Tipo'] == 'Oferta'].resample(interval).agg(ohlcv_agg).dropna()
    df_oferta.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    return df_demanda, df_oferta

def _crear_grafico_vacio(mensaje="Cargando datos. El Scraper podría estar recolectando la información inicial."):
    fig = go.Figure()
    fig.add_annotation(text=mensaje, xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False, font=dict(size=16, color=COLOR_TEXT))
    fig.update_layout(height=350, template="plotly_dark", plot_bgcolor=COLOR_CARD_BACKGROUND, paper_bgcolor=COLOR_CARD_BACKGROUND)
    return fig

# --- VISTA 1: Estilo Trading (Velas) ---
def crear_figura_velas(df_demanda_ohlc, df_oferta_ohlc, interval):
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.03, row_heights=[0.8, 0.2])
    fig.add_trace(go.Candlestick(x=df_demanda_ohlc.index, open=df_demanda_ohlc['Open'], high=df_demanda_ohlc['High'], low=df_demanda_ohlc['Low'], close=df_demanda_ohlc['Close'], name='Demanda (Compra)', increasing_line_color=COLOR_PRECIO_COMPRA, decreasing_line_color=COLOR_PRECIO_COMPRA, line=dict(width=1.5)), row=1, col=1)
    fig.add_trace(go.Candlestick(x=df_oferta_ohlc.index, open=df_oferta_ohlc['Open'], high=df_oferta_ohlc['High'], low=df_oferta_ohlc['Low'], close=df_oferta_ohlc['Close'], name='Oferta (Venta)', increasing_line_color=COLOR_PRECIO_VENTA, decreasing_line_color=COLOR_PRECIO_VENTA, line=dict(width=1.5)), row=1, col=1)
    fig.add_trace(go.Bar(x=df_demanda_ohlc.index, y=df_demanda_ohlc['Volume'], name='Vol. Compra', marker_color=COLOR_VOL_COMPRA, showlegend=False), row=2, col=1)
    fig.add_trace(go.Bar(x=df_oferta_ohlc.index, y=df_oferta_ohlc['Volume'], name='Vol. Venta', marker_color=COLOR_VOL_VENTA, showlegend=False), row=2, col=1)
    fig.update_layout(height=600, template="plotly_dark", hovermode="x unified", title={'text': f'Estilo Trading (Intervalo: {interval})', 'font': dict(size=18, family='Roboto')}, legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5), xaxis_rangeslider_visible=False, plot_bgcolor=COLOR_CARD_BACKGROUND, paper_bgcolor=COLOR_CARD_BACKGROUND, barmode='overlay', margin=dict(t=50, l=10, r=10, b=10))
    fig.update_yaxes(title_text="Precio USDT (VES)", row=1, col=1, gridcolor='rgba(255,255,255,0.08)')
    fig.update_yaxes(title_text="Volumen USDT", row=2, col=1, showgrid=False)
    fig.update_xaxes(gridcolor='rgba(255,255,255,0.08)', row=2, col=1)
    fig.update_xaxes(showticklabels=False, row=1, col=1)
    return fig

# --- VISTA 2: Estilo Analítico (Área de Spread) ---
def crear_figura_spread(df_demanda_ohlc, df_oferta_ohlc, interval):
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.03, row_heights=[0.8, 0.2])
    df_combinado = pd.merge(df_demanda_ohlc[['Close', 'Volume']], df_oferta_ohlc[['Close', 'Volume']], left_index=True, right_index=True, how='outer', suffixes=('_D', '_O'))
    df_combinado['Close_D'] = df_combinado['Close_D'].ffill()
    df_combinado['Close_O'] = df_combinado['Close_O'].ffill()
    df_combinado['Volumen_Total'] = df_combinado['Volume_D'].fillna(0) + df_combinado['Volume_O'].fillna(0)
    fig.add_trace(go.Scatter(x=df_combinado.index, y=df_combinado['Close_D'], mode='lines', line=dict(color=COLOR_PRECIO_COMPRA, width=1.5), name='Demanda (Compra)', hovertemplate='Compra: <b>%{y:.2f} VES</b><extra></extra>'), row=1, col=1)
    fig.add_trace(go.Scatter(x=df_combinado.index, y=df_combinado['Close_O'], mode='lines', line=dict(color=COLOR_PRECIO_VENTA, width=1.5), fill='tonexty', fillcolor=COLOR_SPREAD, name='Oferta (Venta)', hovertemplate='Venta: <b>%{y:.2f} VES</b><extra></extra>'), row=1, col=1)
    fig.add_trace(go.Bar(x=df_combinado.index, y=df_combinado['Volumen_Total'], name='Volumen Total', marker_color=COLOR_VOL_TOTAL, showlegend=False), row=2, col=1)
    fig.update_layout(height=600, template="plotly_dark", hovermode="x unified", title={'text': f'Estilo Analítico (Intervalo: {interval})', 'font': dict(size=18, family='Roboto')}, legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5), xaxis_rangeslider_visible=False, plot_bgcolor=COLOR_CARD_BACKGROUND, paper_bgcolor=COLOR_CARD_BACKGROUND, margin=dict(t=50, l=10, r=10, b=10))
    fig.update_yaxes(title_text="Precio USDT (VES)", row=1, col=1, gridcolor='rgba(255,255,255,0.08)')
    fig.update_yaxes(title_text="Volumen USDT", row=2, col=1, showgrid=False)
    fig.update_xaxes(gridcolor='rgba(255,255,255,0.08)', row=2, col=1)
    fig.update_xaxes(showticklabels=False, row=1, col=1)
    return fig

# --- VISTA 3: Estilo Moderno (Línea/Burbuja) ---
def crear_figura_burbuja(df_demanda_ohlc, df_oferta_ohlc, interval):
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
    fig.update_layout(height=600, template="plotly_dark", hovermode="x unified", title={'text': f'Estilo Moderno (Intervalo: {interval})', 'font': dict(size=18, family='Roboto')}, legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5), xaxis_rangeslider_visible=False, plot_bgcolor=COLOR_CARD_BACKGROUND, paper_bgcolor=COLOR_CARD_BACKGROUND, margin=dict(t=50, l=10, r=10, b=10))
    fig.update_yaxes(title_text="Precio USDT (VES)", row=1, col=1, gridcolor='rgba(255,255,255,0.08)')
    fig.update_yaxes(title_text="Volumen USDT", row=2, col=1, showgrid=False)
    fig.update_xaxes(gridcolor='rgba(255,255,255,0.08)', row=2, col=1)
    fig.update_xaxes(showticklabels=False, row=1, col=1)
    return fig

# --- GRÁFICOS DE MÉTODOS (Funciones simplificadas) ---
def crear_grafico_premium(df_metodos_expl, fecha_inicio, fecha_fin):
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
    fig.update_layout(height=400, template="plotly_dark", barmode='group', title={'text': f'1. Premium: Precio Promedio por Método (Rango: {rango_titulo})', 'font': dict(size=18, color=COLOR_TEXT, family='Roboto')}, legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5), xaxis=dict(title='Precio Promedio (VES)', gridcolor='rgba(255,255,255,0.08)'), yaxis=dict(title='Métodos (Top 10 por Volumen)', showgrid=False, categoryorder='array', categoryarray=df_demanda['Metodos_Pago']), plot_bgcolor=COLOR_CARD_BACKGROUND, paper_bgcolor=COLOR_CARD_BACKGROUND, margin=dict(l=100, t=50, r=10, b=10))
    return fig

def crear_grafico_flujo(df_metodos_expl, fecha_inicio, fecha_fin):
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
    fig.update_layout(height=400, template="plotly_dark", barmode='stack', title={'text': '2. Flujo: Volumen por Método (Oferta vs. Demanda)', 'font': dict(size=18, color=COLOR_TEXT, family='Roboto')}, xaxis=dict(title='Volumen Total (USDT)', gridcolor='rgba(255,255,255,0.08)'), yaxis=dict(title='Métodos (Top 10)', showgrid=False), plot_bgcolor=COLOR_CARD_BACKGROUND, paper_bgcolor=COLOR_CARD_BACKGROUND, legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5), margin=dict(l=100, t=50, r=10, b=10))
    return fig

def crear_grafico_tendencia(df_metodos_expl, fecha_inicio, fecha_fin):
    df_filtrado = df_metodos_expl[(df_metodos_expl['Timestamp'] >= fecha_inicio) & (df_metodos_expl['Timestamp'] <= fecha_fin)]
    if df_filtrado.empty: return _crear_grafico_vacio()
    duration_days = (fecha_fin - fecha_inicio).days
    if duration_days <= 2: interval, interval_label = '1H', "1 Hora"
    elif duration_days <= 14: interval, interval_label = '6H', "6 Horas"
    else: interval, interval_label = '1D', "1 Día"
    top_metodos = df_filtrado.groupby('Metodos_Pago')['Volumen'].sum().nlargest(7).index
    df_filtrado['Metodo_Agrupado'] = df_filtrado['Metodos_Pago'].apply(lambda x: x if x in top_metodos else 'Otros')
    df_resampled = (df_filtrado.set_index('Timestamp').groupby('Metodo_Agrupado').resample(interval)['Volumen'].sum().unstack(level=0, fill_value=0))
    if 'Otros' in df_resampled.columns: df_resampled = df_resampled[[col for col in df_resampled if col != 'Otros'] + ['Otros']]
    fig = go.Figure()
    for i, metodo in enumerate(df_resampled.columns):
        color = PALETA_METODOS[i % len(PALETA_METODOS)] if metodo != 'Otros' else '#7F8C8D'
        fig.add_trace(go.Scatter(x=df_resampled.index, y=df_resampled[metodo], name=metodo, mode='lines', line=dict(width=0.5, color=color), stackgroup='one', groupnorm='percent', hovertemplate=f'<b>{metodo}</b><br>%{{y:.1f}}%<extra></extra>'))
    fig.update_layout(height=400, template="plotly_dark", title={'text': f'3. Tendencia: Cuota de Mercado (Intervalo: {interval_label})', 'font': dict(size=18, color=COLOR_TEXT, family='Roboto')}, xaxis=dict(title='Fecha', gridcolor='rgba(255,255,255,0.08)'), yaxis=dict(title='Cuota de Mercado (%)', showgrid=False, ticksuffix='%'), plot_bgcolor=COLOR_CARD_BACKGROUND, paper_bgcolor=COLOR_CARD_BACKGROUND, legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5), hovermode='x unified', margin=dict(l=100, t=50, r=10, b=10))
    return fig

# --- 3. FUNCIONES AUXILIARES ---

def obtener_rango_fechas_del_grafico(relayout_data, df_ohlc_actual):
    if relayout_data is None or 'xaxis.range[0]' not in relayout_data:
        if not df_ohlc_actual.empty: return df_ohlc_actual.index.min(), df_ohlc_actual.index.max()
        else: return pd.Timestamp.now(), pd.Timestamp.now()
    try:
        fecha_inicio_str, fecha_fin_str = relayout_data['xaxis.range[0]'], relayout_data['xaxis.range[1]']
        return pd.to_datetime(fecha_inicio_str), pd.to_datetime(fecha_fin_str)
    except Exception:
        return df_ohlc_actual.index.min(), df_ohlc_actual.index.max()

def crear_texto_rango_fechas(fecha_inicio, fecha_fin):
    return html.Span([html.Span("RANGO DE FECHA: ", style={'color': 'white', 'fontWeight': '400'}), html.Span(f"{fecha_inicio.strftime(DEFAULT_TIMESTAMP_FORMAT)}", style={'color': COLOR_PRECIO_COMPRA, 'fontWeight': '700'}), html.Span(" — ", style={'color': 'gray'}), html.Span(f"{fecha_fin.strftime(DEFAULT_TIMESTAMP_FORMAT)}", style={'color': COLOR_PRECIO_VENTA, 'fontWeight': '700'})])


# --- 4. INICIALIZACIÓN DE DASH Y CARGA DE DATOS ---

app = Dash(__name__, external_stylesheets=EXTERNAL_STYLESHEET)
# ¡La variable importante que Gunicorn buscará!
server = app.server 

if ENGINE is None:
    app.layout = html.Div([html.H1("Error Fatal de Conexión", style={'textAlign': 'center', 'color': 'red'}), html.P("No se pudo conectar a la base de datos. Verifica la variable de entorno DATABASE_URL.", style={'textAlign': 'center', 'color': 'white'})], className='container')
else:
    # Carga inicial de datos (puede estar vacía al principio)
    df_raw_global, df_metodos_expl_global = cargar_datos_crudos() 
    df_ohlc_demanda_global, df_ohlc_oferta_global = crear_datos_ohlc(df_raw_global, DEFAULT_INTERVAL)

    figura_principal_inicial = crear_figura_velas(df_ohlc_demanda_global, df_ohlc_oferta_global, DEFAULT_INTERVAL)
    if not df_ohlc_demanda_global.empty:
        fecha_inicio_inicial, fecha_fin_inicial = (df_ohlc_demanda_global.index.min(), df_ohlc_demanda_global.index.max())
    else:
        fecha_inicio_inicial, fecha_fin_inicial = pd.Timestamp.now(), pd.Timestamp.now()
        
    figura_premium_inicial = crear_grafico_premium(df_metodos_expl_global, fecha_inicio_inicial, fecha_fin_inicial)
    figura_flujo_inicial = crear_grafico_flujo(df_metodos_expl_global, fecha_inicio_inicial, fecha_fin_inicial)
    figura_tendencia_inicial = crear_grafico_tendencia(df_metodos_expl_global, fecha_inicio_inicial, fecha_fin_inicial)
    texto_fecha_inicial = crear_texto_rango_fechas(fecha_inicio_inicial, fecha_fin_inicial)

    # --- 5. LAYOUT DE LA APLICACIÓN ---
    app.layout = html.Div([
        html.H1("Análisis de Mercado P2P Modular"),
        
        html.Div(className='interval-selector', children=[
            dcc.RadioItems(id='interval-selector', options=[{'label': '15 Minutos', 'value': '15T'}, {'label': '1 Hora', 'value': '1H'}, {'label': '4 Horas', 'value': '4H'}, {'label': '1 Día', 'value': '1D'},], value=DEFAULT_INTERVAL, labelStyle={'display': 'inline-block', 'margin': '0 15px', 'color': COLOR_TEXT, 'fontSize': '1.1em'})
        ]),
        
        dcc.Tabs(id="tabs-grafico-principal", value=DEFAULT_CHART_TYPE, className='tabs-container', children=[
            dcc.Tab(label='Estilo Trading (Velas)', value='tab-velas', className='custom-tab', selected_className='custom-tab--selected'),
            dcc.Tab(label='Estilo Analítico (Spread)', value='tab-spread', className='custom-tab', selected_className='custom-tab--selected'),
            dcc.Tab(label='Estilo Moderno (Burbuja)', value='tab-burbuja', className='custom-tab', selected_className='custom-tab--selected'),
        ]),
        
        dcc.Graph(id='grafico-principal', figure=figura_principal_inicial, config={'scrollZoom': True}),
        
        html.Div(id='output-rango-fecha', children=texto_fecha_inicial),
        
        html.Details(open=False, children=[html.Summary(html.B("💳 Análisis Avanzado de Métodos de Pago")), html.Div(className='graph-container', children=[dcc.Graph(id='grafico-metodos-premium', figure=figura_premium_inicial, config={'scrollZoom': True}), html.Hr(className='graph-separator'), dcc.Graph(id='grafico-metodos-flujo', figure=figura_flujo_inicial, config={'scrollZoom': True}), html.Hr(className='graph-separator'), dcc.Graph(id='grafico-metodos-tendencia', figure=figura_tendencia_inicial, config={'scrollZoom': True})])]),
        
    ], className='container') 


# --- 6. CALLBACKS ---

@app.callback(
    Output('grafico-principal', 'figure'),
    Output('grafico-metodos-premium', 'figure'),
    Output('grafico-metodos-flujo', 'figure'),
    Output('grafico-metodos-tendencia', 'figure'),
    Output('output-rango-fecha', 'children'),
    Input('tabs-grafico-principal', 'value'), 
    Input('interval-selector', 'value'),       
    Input('grafico-principal', 'relayoutData') 
)
def actualizar_graficos(tab_value, interval_value, relayout_data):
    
    df_raw_global_callback, df_metodos_expl_global_callback = cargar_datos_crudos()
    
    if df_raw_global_callback.empty:
        fig_vacia = _crear_grafico_vacio()
        texto_fecha_vacio = crear_texto_rango_fechas(pd.Timestamp.now(), pd.Timestamp.now())
        return fig_vacia, fig_vacia, fig_vacia, fig_vacia, texto_fecha_vacio

    ctx = callback_context
    trigger_id = ctx.triggered_id
    
    df_demanda_ohlc, df_oferta_ohlc = crear_datos_ohlc(df_raw_global_callback, interval_value)

    if trigger_id == 'grafico-principal':
        fecha_inicio, fecha_fin = obtener_rango_fechas_del_grafico(relayout_data, df_demanda_ohlc)
    else:
        if not df_demanda_ohlc.empty:
            fecha_inicio, fecha_fin = df_demanda_ohlc.index.min(), df_demanda_ohlc.index.max()
        else:
            fecha_inicio, fecha_fin = pd.Timestamp.now(), pd.Timestamp.now()

    if trigger_id == 'grafico-principal':
        fig_principal = dash.no_update
    else:
        if tab_value == 'tab-velas':
            fig_principal = crear_figura_velas(df_demanda_ohlc, df_oferta_ohlc, interval_value)
        elif tab_value == 'tab-spread':
            fig_principal = crear_figura_spread(df_demanda_ohlc, df_oferta_ohlc, interval_value)
        elif tab_value == 'tab-burbuja':
            fig_principal = crear_figura_burbuja(df_demanda_ohlc, df_oferta_ohlc, interval_value)
        else:
            fig_principal = crear_figura_velas(df_demanda_ohlc, df_oferta_ohlc, interval_value)

    fig_premium = crear_grafico_premium(df_metodos_expl_global_callback, fecha_inicio, fecha_fin)
    fig_flujo = crear_grafico_flujo(df_metodos_expl_global_callback, fecha_inicio, fecha_fin)
    fig_tendencia = crear_grafico_tendencia(df_metodos_expl_global_callback, fecha_inicio, fecha_fin)
    
    texto_fecha = crear_texto_rango_fechas(fecha_inicio, fecha_fin)
    
    return fig_principal, fig_premium, fig_flujo, fig_tendencia, texto_fecha


# --- EJECUCIÓN ---
if __name__ == '__main__':
    if ENGINE is not None:
        app.run(debug=True, host='0.0.0.0', port=8050)
    else:
        print("❌ La aplicación no se inició. Error de conexión a la BD.")
