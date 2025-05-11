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
driver = '{ODBC Driver 17 for SQL Server}'
conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'

# --- Start ETL ---
def main():
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    try:
        blob_client = container_client.get_blob_client("coach_data.csv")
        file_data = blob_client.download_blob().readall()

        # Read CSV with header on the second row (Excel row 2)
        df = pd.read_csv(io.BytesIO(file_data), header=1)
        df.columns = df.columns.str.strip()

        # Drop rows without coach names
        print("📥 Processing coach_data.csv")

        for _, row in df.iterrows():
            try:
                name = str(row.get("Coach", "")).strip()
                experience = int(row.get("Yrs", 0)) if pd.notna(row.get("Yrs")) else 0
                contract_date = None  # No Contract_Current column in this file

                # Check for existing coach by name
                cursor.execute("SELECT Coach_ID FROM Coach WHERE LOWER(Name) = LOWER(?)", name)
                exists = cursor.fetchone()

                if exists:
                    coach_id = exists[0]
                    cursor.execute("""
                        UPDATE Coach
                        SET Years_Experience = ?
                        WHERE Coach_ID = ?
                    """, experience, coach_id)
                    print(f"🔁 Updated coach: {name}")
                else:
                    cursor.execute("""
                        INSERT INTO Coach (Name, Years_Experience, Contract_Current)
                        VALUES (?, ?, ?)
                    """, name, experience, contract_date)
                    print(f"✅ Inserted coach: {name}")

            except Exception as e:
                print(f"❌ Error processing coach '{row.get('Coach')}': {e}")

    except Exception as e:
        print(f"❌ Failed to read coach_data.csv: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print("🎉 Coach data upload complete.")

# Run the script
if __name__ == "__main__":
    main()
