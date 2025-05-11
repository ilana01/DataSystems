import pandas as pd
from azure.storage.blob import BlobServiceClient
import pyodbc
import io

# --- Azure Blob Storage Config ---
AZURE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=datadraft;AccountKey=nqr6YGXwxU+Lb+Yq+J2BBxTlXBH0FZTfgLqba4LdMjfXTaJyMKKIknmXhLjco/Hx7airCXTkEeu5+ASt+cfJKw==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "datadraftdata"
BLOB_NAME = "2025_Recruitment_Event_Players.csv"

# --- SQL Server Config ---
server = 'datadraft.database.windows.net'
database = 'DataDraft_Database'
username = 'datadraft'
password = 'Datasystems2025'
driver = '{ODBC Driver 18 for SQL Server}'
conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'

# Event mapping
EVENT_IDS = {
    "2025 G League Elite Camp": 1,
    "2025 NBA Draft Combine": 2
}

def main():
    # Azure Blob and DB setup
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    blob_client = blob_service_client.get_container_client(CONTAINER_NAME).get_blob_client(BLOB_NAME)
    file_data = blob_client.download_blob().readall()
    df = pd.read_csv(io.BytesIO(file_data))

    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    for _, row in df.iterrows():
        try:
            # Prepare values
            full_name = str(row['Player_Name']).strip().title()
            college_name = str(row['College']).strip().title() if pd.notna(row['College']) else None
            event_name = str(row['Event']).strip()
            event_id = EVENT_IDS.get(event_name)

            if not event_id:
                print(f"Skipping unknown event: {event_name}")
                continue

            # Handle College
            college_id = None
            if college_name:
                cursor.execute("SELECT College_ID FROM College WHERE LOWER(College_Name) = LOWER(?)", college_name)
                college_row = cursor.fetchone()
                if college_row:
                    college_id = college_row[0]
                else:
                    cursor.execute("SELECT ISNULL(MAX(College_ID), 0) + 1 FROM College")
                    college_id = cursor.fetchone()[0]
                    cursor.execute("INSERT INTO College (College_ID, College_Name) VALUES (?, ?)", college_id, college_name)
                    print(f"Inserted new college: {college_name} (ID {college_id})")

            # Handle Player
            cursor.execute("SELECT Player_ID FROM Player WHERE LOWER(Player_Name) = LOWER(?)", full_name)
            player_row = cursor.fetchone()
            if player_row:
                player_id = player_row[0]
                if college_id:
                    cursor.execute("UPDATE Player SET College_ID = ? WHERE Player_ID = ?", college_id, player_id)
            else:
                cursor.execute("SELECT ISNULL(MAX(Player_ID), 0) + 1 FROM Player")
                player_id = cursor.fetchone()[0]
                cursor.execute(
                    "INSERT INTO Player (Player_Name, College_ID) VALUES (?, ?)",
                     full_name, college_id
                )
                print(f"Inserted new player: {full_name} (ID {player_id})")

            # Handle Recruitment_Event_Player
            cursor.execute("""
                IF NOT EXISTS (
                    SELECT 1 FROM Recruitment_Event_Player WHERE Event_ID = ? AND Player_ID = ?
                )
                INSERT INTO Recruitment_Event_Player (Event_ID, Player_ID) VALUES (?, ?)
            """, event_id, player_id, event_id, player_id)

        except Exception as e:
            print(f"Error processing player {row['Player_Name']}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print("Player, college, and event attendance update complete.")

if __name__ == "__main__":
    main()
