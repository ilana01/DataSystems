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

# --- Height conversion: "6-3" -> 75 inches ---
def parse_height(feet_inches):
    if isinstance(feet_inches, str) and '-' in feet_inches:
        try:
            feet, inches = feet_inches.split('-')
            return int(feet) * 12 + int(inches)
        except ValueError:
            return None
    return None

def main():
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # --- Download and read common_player_info.csv ---
    blob_client = container_client.get_blob_client("archive/csv/common_player_info.csv")
    file_data = blob_client.download_blob().readall()
    df = pd.read_csv(io.BytesIO(file_data))

    for _, row in df.iterrows():
        try:
            f_name = str(row.get('first_name', '')).strip().title()
            l_name = str(row.get('last_name', '')).strip().title()
            full_name = f"{f_name} {l_name}"

            dob = row.get('birthdate')
            nationality = row.get('country')
            height_raw = row.get('height', '')
            height = parse_height(height_raw)
            weight = int(row.get('weight', 0)) if not pd.isnull(row.get('weight')) else None

            # Lookup by name
            cursor.execute("""
                SELECT Player_ID FROM Player WHERE LOWER(Player_Name) = LOWER(?)
            """, full_name)
            result = cursor.fetchone()

            if result:
                player_id = result[0]
                cursor.execute("""
                    UPDATE Player
                    SET DOB = ?, Nationality = ?, Height = ?, Weight = ?
                    WHERE Player_ID = ?
                """, dob, nationality, height, weight, player_id)
                print(f"Updated existing player: {full_name} (ID {player_id})")
                conn.commit()
            else:
                cursor.execute("""
                    INSERT INTO Player (Player_Name, DOB, Nationality, Height, Weight)
                    OUTPUT INSERTED.Player_ID
                    VALUES (?, ?, ?, ?, ?)
                """, full_name, dob, nationality, height, weight)
                player_id = cursor.fetchone()[0]
                print(f"Inserted new player: {full_name} (ID {player_id})")
                conn.commit()

        except Exception as e:
            print(f"Error processing player {row.get('first_name')} {row.get('last_name')}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print("\nPlayer import from common_player_info.csv complete.")

if __name__ == "__main__":
    main()
