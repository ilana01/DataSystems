import pandas as pd
import pyodbc
from azure.storage.blob import BlobServiceClient
import io

# --- Azure Blob Storage Config ---
AZURE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=datadraft;AccountKey=nqr6YGXwxU+Lb+Yq+J2BBxTlXBH0FZTfgLqba4LdMjfXTaJyMKKIknmXhLjco/Hx7airCXTkEeu5+ASt+cfJKw==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "datadraftdata"
PLAYER_AGENT_FILE = "Agents.csv"
AGENT_PROFILE_FILE = "Cleaned_AgentData.csv"

# --- SQL Server Config ---
server = 'datadraft.database.windows.net'
database = 'DataDraft_Database'
username = 'datadraft'
password = 'Datasystems2025'
driver = '{ODBC Driver 18 for SQL Server}'
conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'

def convert_total_to_float(value):
    try:
        if pd.isna(value):
            return None
        value = value.strip().replace('$', '').upper()
        if value.endswith('B'):
            return float(value[:-1]) * 1_000_000_000
        elif value.endswith('M'):
            return float(value[:-1]) * 1_000_000
        else:
            return float(value)
    except:
        return None

def main():
    # Connect to Azure Blob
    blob_service = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container_client = blob_service.get_container_client(CONTAINER_NAME)

    # Load player-agent map
    player_agent_blob = container_client.get_blob_client(PLAYER_AGENT_FILE)
    player_agent_data = pd.read_csv(io.BytesIO(player_agent_blob.download_blob().readall()))

    # Load cleaned agent profiles with agency info
    profile_blob = container_client.get_blob_client(AGENT_PROFILE_FILE)
    agent_profiles = pd.read_csv(io.BytesIO(profile_blob.download_blob().readall()))
    agent_profiles["Agent"] = agent_profiles["Agent"].astype(str).str.strip()

    # Connect to SQL Server
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    for _, row in player_agent_data.iterrows():
        try:
            player_name = str(row.get('Player', '')).strip()
            agent_name = str(row.get('Agent', '')).strip()

            # Get agency, contract count, and total value
            agency_row = agent_profiles[agent_profiles["Agent"].str.lower() == agent_name.lower()]
            agency_name = agency_row["Agency"].values[0] if not agency_row.empty else None
            contract_count = int(agency_row["Contracts"].values[0]) if not agency_row.empty and not pd.isna(agency_row["Contracts"].values[0]) else None
            total_str = agency_row["Total"].values[0] if not agency_row.empty else None
            rating = convert_total_to_float(total_str)

            if agency_name is not None and isinstance(agency_name, str) and agency_name.strip() == "":
                agency_name = None

            # Check if agent exists
            cursor.execute("SELECT Agent_ID FROM Agent WHERE LOWER(Name) = LOWER(?)", agent_name)
            agent_result = cursor.fetchone()

            if agent_result:
                agent_id = agent_result[0]
                cursor.execute("""
                    UPDATE Agent
                    SET Agency_Name = ?, Contract_Count = ?, Rating = ?
                    WHERE Agent_ID = ?
                """, agency_name, contract_count, rating, agent_id)
                print(f"Updated agent: {agent_name} (Agency: {agency_name}, Contracts: {contract_count}, Rating: {rating})")
                conn.commit()
            else:
                cursor.execute("SELECT ISNULL(MAX(Agent_ID), 0) + 1 FROM Agent")
                agent_id = cursor.fetchone()[0]
                cursor.execute("""
                    INSERT INTO Agent (Agent_ID, Name, Agency_Name, Contract_Count, Rating)
                    VALUES (?, ?, ?, ?, ?)
                """, agent_id, agent_name, agency_name, contract_count, rating)
                print(f"Inserted agent: {agent_name} (Agency: {agency_name}, Contracts: {contract_count}, Rating: {rating})")
                conn.commit()

            # Link agent to player
            cursor.execute("SELECT Player_ID FROM Player WHERE LOWER(Player_Name) = LOWER(?)", player_name)
            player_result = cursor.fetchone()

            if player_result:
                player_id = player_result[0]
                cursor.execute("UPDATE Player SET Agent_ID = ? WHERE Player_ID = ?", agent_id, player_id)
                print(f"Linked agent '{agent_name}' to player '{player_name}'")
                conn.commit()
            else:
                print(f"Player not found: {player_name}")

        except Exception as e:
            print(f"Error processing player {row.get('Player', '[UNKNOWN]')}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print("Finished uploading agent data and linking to players.")

if __name__ == "__main__":
    main()
