import pandas as pd
from azure.storage.blob import BlobServiceClient
import pyodbc
import io

# --- Azure Blob Storage Config ---
AZURE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=datadraft;AccountKey=nqr6YGXwxU+Lb+Yq+J2BBxTlXBH0FZTfgLqba4LdMjfXTaJyMKKIknmXhLjco/Hx7airCXTkEeu5+ASt+cfJKw==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "datadraftdata"

# --- SQL Server Config ---
server = 'datadraft.database.windows.net'
database = 'DataDraft_Database'
username = 'datadraft'
password = 'Datasystems2025'
driver = '{ODBC Driver 18 for SQL Server}'
conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'

def main():
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # --- Download and read the common_player_info.csv file ---
    blob_client = container_client.get_blob_client("archive/csv/common_player_info.csv")
    file_data = blob_client.download_blob().readall()
    df = pd.read_csv(io.BytesIO(file_data))

    for _, row in df.iterrows():
        try:
            f_name = str(row.get('first_name', '')).strip().title()
            l_name = str(row.get('last_name', '')).strip().title()
            full_name = f"{f_name} {l_name}"

            team_abbr = row.get('team_abbreviation')
            if not team_abbr:
                print(f"⚠️ No team abbreviation for player {full_name}")
                continue

            # Find player by full name
            cursor.execute("SELECT Player_ID FROM Player WHERE LOWER(Player_Name) = LOWER(?)", full_name)
            player_row = cursor.fetchone()
            if not player_row:
                print(f"  Player '{full_name}' not found in database.")
                continue

            player_id = player_row[0]

            # Find team by abbreviation
            cursor.execute("SELECT Team_ID FROM Team WHERE LOWER(Abbreviation) = LOWER(?)", team_abbr.strip().lower())
            team_row = cursor.fetchone()
            if not team_row:
                print(f"  Team with abbreviation '{team_abbr}' not found.")
                continue

            team_id = team_row[0]

            # Update player with team ID
            cursor.execute("UPDATE Player SET Team_ID = ? WHERE Player_ID = ?", team_id, player_id)
            print(f"  Linked player {full_name} to team '{team_abbr}' (ID {team_id})")

        except Exception as e:
            print(f"  Error linking player {row.get('first_name')} {row.get('last_name')}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print("  Player-team linking complete.")

if __name__ == "__main__":
    main()
