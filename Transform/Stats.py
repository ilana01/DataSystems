import pandas as pd
import pyodbc
from azure.storage.blob import BlobServiceClient
import io

# --- Azure Blob Storage Config ---
AZURE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=datadraft;AccountKey=nqr6YGXwxU+Lb+Yq+J2BBxTlXBH0FZTfgLqba4LdMjfXTaJyMKKIknmXhLjco/Hx7airCXTkEeu5+ASt+cfJKw==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "datadraftdata"
BLOB_NAME = "NBA_Stats.csv"

# --- SQL Server Config ---
server = 'datadraft.database.windows.net'
database = 'DataDraft_Database'
username = 'datadraft'
password = 'Datasystems2025'
driver = '{ODBC Driver 18 for SQL Server}'
conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'

def main():
    # Connect to Azure Blob
    blob_service = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    blob_client = blob_service.get_container_client(CONTAINER_NAME).get_blob_client(BLOB_NAME)
    blob_data = blob_client.download_blob().readall()

    # Load CSV into DataFrame
    df = pd.read_csv(io.BytesIO(blob_data))

    # Connect to SQL Server
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    season = "2023-24"  # Fixed season since it's not in the CSV

    for _, row in df.iterrows():
        try:
            name = str(row.get('NAME', '')).strip()

            # Get stats
            ppg = int(row.get('PPG', 0))
            apg = int(row.get('APG', 0))
            rpg = int(row.get('RPG', 0))
            bpg = int(row.get('BPG', 0))
            spg = int(row.get('SPG', 0))

            ft_pct = float(row.get('FT%', 0.0)) * 100 if row.get('FT%') <= 1 else float(row.get('FT%', 0.0))
            fg_pct = float(row.get('eFG%', 0.0)) * 100 if row.get('eFG%') <= 1 else float(row.get('eFG%', 0.0))
            tp_pct = float(row.get('3P%', 0.0)) * 100 if row.get('3P%') <= 1 else float(row.get('3P%', 0.0))

            # Find player in database
            cursor.execute("SELECT Player_ID FROM Player WHERE LOWER(Player_Name) = LOWER(?)", name.lower())
            result = cursor.fetchone()
            if not result:
                print(f"❌ Player not found: {name}")
                continue

            player_id = result[0]

            # Insert or update performance statistics
            cursor.execute("""
                MERGE Performance_Statistics AS target
                USING (SELECT ? AS Player_ID, ? AS Season) AS source
                ON target.Player_ID = source.Player_ID AND target.Season = source.Season
                WHEN MATCHED THEN
                    UPDATE SET Points_Per_Game = ?, Assists_Per_Game = ?, Rebounds_Per_Game = ?, Blocks_Per_Game = ?,
                               Steals_Per_Game = ?, Free_Throw_Percentage = ?, Field_Goal_Percentage = ?, 
                               Three_Points_Percentage = ?
                WHEN NOT MATCHED THEN
                    INSERT (Player_ID, Season, Points_Per_Game, Assists_Per_Game, Rebounds_Per_Game, Blocks_Per_Game,
                            Steals_Per_Game, Free_Throw_Percentage, Field_Goal_Percentage, Three_Points_Percentage)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
                player_id, season, ppg, apg, rpg, bpg, spg, ft_pct, fg_pct, tp_pct,
                player_id, season, ppg, apg, rpg, bpg, spg, ft_pct, fg_pct, tp_pct
            )

            print(f"✅ Stats updated for: {name} (Season: {season})")

        except Exception as e:
            print(f"❌ Error processing {row.get('NAME')}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print("🎉 Finished uploading player performance statistics.")

if __name__ == "__main__":
    main()
