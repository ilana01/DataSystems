import pandas as pd
import pyodbc
from azure.storage.blob import BlobServiceClient
import io

# --- Azure Blob Storage Config ---
AZURE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=datadraft;AccountKey=nqr6YGXwxU+Lb+Yq+J2BBxTlXBH0FZTfgLqba4LdMjfXTaJyMKKIknmXhLjco/Hx7airCXTkEeu5+ASt+cfJKw==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "datadraftdata"
BLOB_NAME = "Team Summaries.csv"

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

    for _, row in df.iterrows():
        try:
            league_name = str(row.get("lg", "")).strip()
            team_name = str(row.get("team", "")).strip()
            abbreviation = str(row.get("abbreviation", "")).strip()
            location = str(row.get("arena", "")).strip()  # maps to 'Location' in Team table

            # Check if league exists
            cursor.execute("SELECT League_ID FROM League WHERE LOWER(League_Name) = LOWER(?)", league_name)
            league_row = cursor.fetchone()

            if league_row:
                league_id = league_row[0]
            else:
                # Generate new League_ID manually
                cursor.execute("SELECT ISNULL(MAX(League_ID), 0) + 1 FROM League")
                league_id = cursor.fetchone()[0]

                # Insert new League with default location "USA"
                cursor.execute("""
                    INSERT INTO League (League_ID, League_Name, League_Location)
                    VALUES (?, ?, ?)
                """, league_id, league_name, "USA")
                print(f"Inserted league: {league_name} (ID {league_id})")

            # Insert or update team using abbreviation
            cursor.execute("SELECT Team_ID FROM Team WHERE LOWER(Abbreviation) = LOWER(?)", abbreviation)
            team_row = cursor.fetchone()
            if not team_row:
                cursor.execute("SELECT ISNULL(MAX(Team_ID), 0) + 1 FROM Team")
                team_id = cursor.fetchone()[0]
                cursor.execute("""
                    INSERT INTO Team (Team_ID, Team_Name, Abbreviation, Team_Location, League_ID)
                    VALUES (?, ?, ?, ?, ?)
                """,team_id, team_name, abbreviation, location, league_id)
                print(f"Inserted team: {team_name} ({abbreviation})")
            else:
                cursor.execute("""
                    UPDATE Team
                    SET Team_Name = ?, Team_Location = ?, League_ID = ?
                    WHERE LOWER(Abbreviation) = LOWER(?)
                """, team_name, location, league_id, abbreviation)
                print(f"Updated team: {team_name} ({abbreviation})")

        except Exception as e:
            print(f"Error processing team {abbreviation}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print("Finished uploading teams and linking leagues.")

if __name__ == "__main__":
    main()
