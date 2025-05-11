import gradio as gr
import pandas as pd
import pyodbc
import plotly.express as px
from dotenv import load_dotenv

# Connection info
server = 'datadraft.database.windows.net'
database = 'DataDraft_Database'
username = 'datadraft'
password = 'Datasystems2025'

def get_connection():
    driver = '{ODBC Driver 18 for SQL Server}'
    conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    return pyodbc.connect(conn_str)

def fetch_data(min_steals):
    conn = get_connection()
    query = """
        SELECT Player_ID, Season, Points_Per_Game, Assists_Per_Game, Steals_Per_Game
        FROM dbo.Performance_Statistics
        WHERE Steals_Per_Game >= ?
    """
    df = pd.read_sql(query, conn, params=[min_steals])
    conn.close()
    return df

def show_data(min_steals):
    df = fetch_data(min_steals)
    if df.empty:
        return df, None
    fig = px.bar(df, x="Player_ID", y="Steals_Per_Game", color="Season", title="Steals Per Game by Player")
    return df, fig

with gr.Blocks() as demo:
    gr.Markdown("Player Performance Explorer")

    steals_slider = gr.Slider(minimum=0, maximum=5, step=0.1, label="Minimum Steals Per Game")
    df_output = gr.Dataframe()
    chart_output = gr.Plot()

    steals_slider.change(fn=show_data, inputs=steals_slider, outputs=[df_output, chart_output])

demo.launch()
