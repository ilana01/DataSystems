import pandas as pd
import pyodbc
from azure.storage.blob import BlobServiceClient
import io

# --- Azure + SQL Config ---
AZURE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=datadraft;AccountKey=nqr6YGXwxU+Lb+Yq+J2BBxTlXBH0FZTfgLqba4LdMjfXTaJyMKKIknmXhLjco/Hx7airCXTkEeu5+ASt+cfJKw==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "datadraftdata"
BLOB_NAME = "NBA_head_coaches.csv"

server = 'datadraft.database.windows.net'
database = 'DataDraft_Database'
username = 'datadraft'
password = 'Datasystems2025'
driver = '{ODBC Driver 18 for SQL Server}'
conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'

def extract_year(season_str):
    try:
        return int(season_str.split('-')[0])
    except:
        return None

def main():
    # Load coach data from Azure
    blob_service = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    blob_client = blob_service.get_container_client(CONTAINER_NAME).get_blob_client(BLOB_NAME)
    data = blob_client.download_blob().readall()
    df = pd.read_csv(io.BytesIO(data))
    df['Name'] = df['Name'].astype(str).str.replace("*", "", regex=False).str.strip()

    # SQL connection
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    for _, row in df.iterrows():
        name = row.get("Name", "").strip()
        start_year = extract_year(row.get("Start season"))
        end_year = extract_year(row.get("End season"))
        years_experience = end_year - start_year + 1 if start_year and end_year else None
        contract_date = f"{end_year}-07-01" if end_year else None  

        # Check if coach exists
        cursor.execute("SELECT Coach_ID FROM Coach WHERE LOWER(Name) = LOWER(?)", name)
        coach_row = cursor.fetchone()

        if coach_row:
            coach_id = coach_row[0]
            cursor.execute("""
                UPDATE Coach
                SET Contract_Current = ?
                WHERE Coach_ID = ?
            """, contract_date, coach_id)
            print(f"Updated coach: {name}")
        else:
            cursor.execute("""
                INSERT INTO Coach (Name, Years_Experience, Contract_Current)
                VALUES ( ?, ?, ?)
            """, name, years_experience, contract_date)
            print(f"Inserted coach: {name}")

        # Handle team assignments
        season = row.get("Start season short")
        team_list = str(row.get("Teams", "")).split(',')
        for team_abbr in team_list:
            team_abbr = team_abbr.strip()
            if team_abbr == "":
                continue

            # Find Team_ID
            cursor.execute("SELECT Team_ID FROM Team WHERE LOWER(Abbreviation) = LOWER(?)", team_abbr)
            team_result = cursor.fetchone()
            if not team_result:
                print(f"Team not found for abbreviation: {team_abbr}")
                continue

            team_id = team_result[0]
            coach_id = cursor.execute("SELECT Coach_ID FROM Coach WHERE LOWER(Name) = LOWER(?)", name).fetchone()[0]

            # Check if assignment already exists
            cursor.execute("""
                SELECT 1 FROM Coach_Team_Assignment
                WHERE Coach_ID = ? AND Team_ID = ? AND Season = ?
            """, coach_id, team_id, season)
            if not cursor.fetchone():
                cursor.execute("""
                    INSERT INTO Coach_Team_Assignment (Coach_ID, Team_ID, Season)
                    VALUES (?, ?, ?)
                """, coach_id, team_id, season)
                print(f"Linked {name} to {team_abbr} ({season})")

    conn.commit()
    cursor.close()
    conn.close()
    print("All coaches processed.")

if __name__ == "__main__":
    main()
