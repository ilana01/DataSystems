import pandas as pd
import json
from azure.storage.blob import BlobServiceClient
import pyodbc
import io

# --- Azure Blob Storage Config ---
AZURE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=datadraft;AccountKey=nqr6YGXwxU+Lb+Yq+J2BBxTlXBH0FZTfgLqba4LdMjfXTaJyMKKIknmXhLjco/Hx7airCXTkEeu5+ASt+cfJKw==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "datadraftdata"

blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

# --- Azure SQL Database Config ---
server = 'datadraft.database.windows.net'
database = 'DataDraft_Database'
username = 'datadraft'
password = 'Datasystems2025'
driver = '{ODBC Driver 18 for SQL Server}'
conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# --- 1. Read nba_leagues_api.json ---
blob_client = container_client.get_blob_client('nba_leagues_api.json')
league_json = json.load(io.BytesIO(blob_client.download_blob().readall()))['response']

league_id = int(league_json['id'])
league_name = league_json['name']
league_location = "USA"  # default location

cursor.execute("""
    IF NOT EXISTS (SELECT 1 FROM League WHERE League_ID = ?)
    INSERT INTO League (League_ID, League_Name, League_Location)
    VALUES (?, ?, ?)
""", league_id, league_id, league_name, league_location)
print("Inserted league.")

# --- 2. Read Nba Player Salaries.csv ---
blob_client = container_client.get_blob_client('Nba Player Salaries.csv')
player_csv_data = blob_client.download_blob().readall()
player_df = pd.read_csv(io.BytesIO(player_csv_data))

# --- Insert Players ---
for _, row in player_df.iterrows():
    try:
        player_id = int(row['Player Id'])
        player_name = row['Player Name']
        salary_str = str(row['2023/2024']).replace('$', '').replace(',', '').strip()
        try:
            salary = int(float(salary_str)) if salary_str and salary_str != '0' else None
        except ValueError:
            salary = None
        age = 22  # Placeholder since age isn't in the CSV

        # Check if player exists
        cursor.execute("SELECT 1 FROM Player WHERE Player_ID = ?", player_id)
        exists = cursor.fetchone()

        if exists:
            cursor.execute("""
                UPDATE Player
                SET Player_Name = ?, Current_Salary = ?, League_ID = ?, Age = ?
                WHERE Player_ID = ?
            """, player_name, salary, league_id, age, player_id)
            print(f"🔁 Updated player: {player_name} (ID {player_id})")
        else:
            cursor.execute("""
                INSERT INTO Player (Player_ID, Player_Name, Current_Salary, League_ID, Age)
                VALUES (?, ?, ?, ?, ?)
            """, player_id, player_name, salary, league_id, age)
            print(f"✅ Inserted player: {player_name} (ID {player_id})")

    except Exception as e:
        print(f"❌ Failed to process player {row.get('Player Name', '[UNKNOWN]')}: {e}")




conn.commit()
cursor.close()
conn.close()
print("All data processed successfully.")
