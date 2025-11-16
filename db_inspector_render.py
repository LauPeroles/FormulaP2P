import dash
from dash import dash_table, html, dcc, Input, Output
import pandas as pd
from sqlalchemy import create_engine
import os
import datetime

# --- CONFIGURACIÓN DE BASE DE DATOS (Lee la variable de entorno de Render) ---
DATABASE_URL = os.environ.get("DATABASE_URL")

if not DATABASE_URL:
    print("[INSPECTOR DB] ERROR FATAL: DATABASE_URL no encontrada en el entorno de Render.")
    exit()

# Asegurar el prefijo correcto para SQLAlchemy
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

try:
    ENGINE = create_engine(DATABASE_URL)
    print("[INSPECTOR DB] Engine de SQLAlchemy creado con la URL interna.")
except Exception as e:
    print(f"[INSPECTOR DB] ERROR FATAL: No se pudo crear el engine: {e}")
    exit()

TABLE_NAME = 'p2p_anuncios' 

# --- APLICACIÓN DASH ---
COLOR_BG = '#282c34'
COLOR_CARD = '#333742'
COLOR_TEXT = '#ffffff'
COLOR_ACCENT = '#00cc96' # Un verde neón para acentuar

app = dash.Dash(__name__)
server = app.server 

# --- CORRECCIÓN DE SINTAXIS EN app.layout ---
# El error era un paréntesis extra al final de la primera línea de html.Div()
app.layout = html.Div(style={'fontFamily': 'Roboto, sans-serif', 'padding': '20px', 'backgroundColor': COLOR_BG, 'minHeight': '100vh', 'color': COLOR_TEXT}, children=[
    html.H1("Inspector de Base de Datos P2P", style={'textAlign': 'center', 'color': COLOR_TEXT}),
    html.P("Esta herramienta se conecta internamente a la base de datos y muestra los 50 registros más recientes.", style={'textAlign': 'center', 'color': '#ccc'}),
    html.P("Si ves datos aquí, confirma que el scraper está funcionando correctamente.", style={'textAlign': 'center', 'color': COLOR_ACCENT, 'fontWeight': 'bold'}),
    
    html.Button('Actualizar Datos (Ejecutar Consulta)', id='btn-refresh-render', n_clicks=0, 
                style={
                    'width': '100%', 'padding': '15px', 'fontSize': '18px', 
                    'backgroundColor': COLOR_ACCENT, 'color': COLOR_BG, 
                    'border': 'none', 'borderRadius': '5px', 'cursor': 'pointer', 'marginBottom': '20px',
                    'fontWeight': 'bold'
                }),
    
    dcc.Loading(id="loading-icon-render", children=[
        html.Div(id='error-output-render', style={'color': 'red', 'marginBottom': '10px', 'textAlign': 'center', 'fontWeight': 'bold'}), 
        dash_table.DataTable(
            id='data-table-render',
            columns=[],
            data=[],
            style_table={'overflowX': 'auto', 'border': f'1px solid {COLOR_ACCENT}', 'borderRadius': '5px'},
            style_header={'backgroundColor': COLOR_CARD, 'color': COLOR_TEXT, 'fontWeight': 'bold'},
            style_cell={
                'backgroundColor': COLOR_CARD, 'color': COLOR_TEXT,
                'fontFamily': 'Roboto, sans-serif',
                'padding': '10px',
                'textAlign': 'left',
                'whiteSpace': 'normal',
                'height': 'auto',
                'border': '1px solid #333'
            },
            page_size=20, # Mostrar 20 filas por página
            sort_action="native",
        )
    ], type="default")
])

@app.callback(
    Output('data-table-render', 'data'),
    Output('data-table-render', 'columns'),
    Output('error-output-render', 'children'),
    Input('btn-refresh-render', 'n_clicks')
)
def update_table(n_clicks):
    print(f"[{datetime.datetime.now()}] [INSPECTOR] Ejecutando consulta a la base de datos... (Click: {n_clicks})")
    try:
        # Consulta SQL para obtener los 50 registros más recientes
        query = f"""
            SELECT id, "Timestamp", "Tipo", "Precio", "Volumen", "Metodos_Pago", "Exchange_Name" 
            FROM {TABLE_NAME} 
            ORDER BY "Timestamp" DESC 
            LIMIT 50
        """
        
        # Leemos los datos desde la DB usando la conexión interna de Render
        df = pd.read_sql(query, con=ENGINE)
        
        if not df.empty:
            df['Timestamp'] = df['Timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
            
        data = df.to_dict('records')
        columns = [{"name": i, "id": i} for i in df.columns]
        
        print(f"[{datetime.datetime.now()}] [INSPECTOR] Tabla actualizada con {len(df)} registros.")
        return data, columns, "" # Sin error
        
    except Exception as e:
        error_message = f"Error Crítico: {e}. Confirma que el Cron Job esté guardando datos."
        print(f"[{datetime.datetime.now()}] [INSPECTOR] ERROR: {e}")
        return [], [], error_message

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=os.environ.get('PORT', 8050))
