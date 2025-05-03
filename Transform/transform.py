import pandas as pd
import json
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

# --- Main ETL Process ---
def main():
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    # # --- 1. Insert League if not exists ---
    blob_client = container_client.get_blob_client('nba_leagues_api.json')
    league_json = json.load(io.BytesIO(blob_client.download_blob().readall()))['response']

    league_id = int(league_json['id'])
    league_name = league_json['name']
    league_location = "USA"

    cursor.execute("""
        IF NOT EXISTS (SELECT 1 FROM League WHERE League_ID = ?)
        INSERT INTO League (League_ID, League_Name, League_Location)
        VALUES (?, ?, ?)
    """, league_id, league_id, league_name, league_location)
    print("Inserted league.")

    # --- 2. Read Player Salaries ---
    blob_client = container_client.get_blob_client('Nba Player Salaries.csv')
    player_csv_data = blob_client.download_blob().readall()
    player_df = pd.read_csv(io.BytesIO(player_csv_data))

    #for _, row in player_df.iterrows():
    for _, row in player_df.head(10).iterrows():
        try:
            player_name = row['Player Name']
            salary_str = str(row['2023/2024']).replace('$', '').replace(',', '').strip()
            try:
                salary = int(float(salary_str)) if salary_str and salary_str != '0' else None
            except ValueError:
                salary = None

            # Find by name
            cursor.execute("""
                SELECT Player_ID FROM Player WHERE LOWER(Player_Name) = LOWER(?)
            """, player_name)
            result = cursor.fetchone()

            if result:
                player_id = result[0]
                cursor.execute("""
                    UPDATE Player
                    SET Current_Salary = ?, League_ID = ?
                    WHERE Player_ID = ?
                """, salary, league_id, player_id)
                print(f"🔁 Updated player: {player_name}")
            else:
                cursor.execute("""
                    INSERT INTO Player (Player_Name, Current_Salary, League_ID)
                    VALUES (?, ?, ?)
                """, player_name, salary, league_id)
                print(f"✅ Inserted player: {player_name}")

        except Exception as e:
            print(f"❌ Failed to process player {row.get('Player Name', '[UNKNOWN]')}: {e}")

# --- 3. Load and process files from archive/csv ---

    archive_path = "archive/csv/"
    blob_list = container_client.list_blobs(name_starts_with=archive_path)

    for blob in blob_list:
        filename = blob.name.split("/")[-1]
        print(f"📄 Processing file: {filename}")

        try:
            blob_client = container_client.get_blob_client(blob)
            file_data = blob_client.download_blob().readall()

            if filename.endswith(".csv"):
                df = pd.read_csv(io.BytesIO(file_data))
            elif filename.endswith(".xls") or filename.endswith(".xlsx"):
                df = pd.read_excel(io.BytesIO(file_data))
            else:
                print(f"⚠️ Skipping unsupported file type: {filename}")
                continue

            # --- Process common_player_info.csv ---
            if filename == "common_player_info.csv":
                df = df.head(1)
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
                            SELECT Player_ID FROM Player
                            WHERE LOWER(Player_Name) = LOWER(?)
                        """, full_name)
                        result = cursor.fetchone()

                        if result:
                            player_id = result[0]
                            cursor.execute("""
                                UPDATE Player
                                SET DOB = ?, Nationality = ?, Height = ?, Weight = ?
                                WHERE Player_ID = ?
                            """, dob, nationality, height, weight, player_id)
                            print(f"🔁 Updated existing player: {full_name} (ID {player_id})")
                        else:
                            cursor.execute("""
                                INSERT INTO Player (Player_Name, DOB, Nationality, Height, Weight)
                                OUTPUT INSERTED.Player_ID
                                VALUES (?, ?, ?, ?, ?)
                            """, full_name, dob, nationality, height, weight)
                            player_id = cursor.fetchone()[0]
                            print(f"✅ Inserted new player: {full_name} (ID {player_id})")

                    except Exception as e:
                        print(f"❌ Error processing player {row.get('first_name')} {row.get('last_name')}: {e}")
            # --- Process game.csv ---
            if filename == "game.csv":
                #df = df.head(1)
                for _, row in df.iterrows():
                    try:
                        game_id = int(row.get('game_id'))
                        game_date = row.get('game_date')
                        location = row.get('arena_name')  # Adjust if necessary
                        teams_involved = row.get('matchup_home')
                        home_pts = row.get('pts_home', '')
                        away_pts = row.get('pts_away', '')
                        game_score = f"{home_pts} - {away_pts}"

                        game_description = row.get('attendance')  # Optional

                        # Check if game already exists
                        cursor.execute("SELECT 1 FROM Game WHERE Game_ID = ?", game_id)
                        exists = cursor.fetchone()

                        if exists:
                            cursor.execute("""
                                UPDATE Game
                                SET Game_Date = ?, Location = ?, Teams_Involved = ?, Game_Score = ?, Game_Description = ?
                                WHERE Game_ID = ?
                            """, game_date, location, teams_involved, game_score, str(game_description), game_id)
                            print(f"🔁 Updated game: {game_id}")
                        else:
                            cursor.execute("""
                                INSERT INTO Game (Game_ID, Game_Date, Location, Teams_Involved, Game_Score, Game_Description)
                                VALUES (?, ?, ?, ?, ?, ?)
                            """, game_id, game_date, location, teams_involved, game_score, str(game_description))
                            print(f"✅ Inserted game: {game_id}")

                    except Exception as e:
                        print(f"❌ Error processing game {row.get('game_id')}: {e}")
                                # --- Process team.csv ---
            if filename == "team.csv":
                df = df.head(1)
                for _, row in df.iterrows():
                    try:
                        team_id = int(row.get('id'))
                        team_name = row.get('full_name')
                        abbreviation = row.get('abbreviation')
                        team_location = row.get('city')  
                        year_founded = int(row.get('year_founded', 0)) if not pd.isnull(row.get('year_founded')) else None
                        league_id = league_id  # use previously set league_id
                        college_id = None  

                        # Check if team exists
                        cursor.execute("SELECT 1 FROM Team WHERE Team_ID = ?", team_id)
                        exists = cursor.fetchone()

                        if exists:
                            cursor.execute("""
                                UPDATE Team
                                SET Team_Name = ?, Team_Location = ?, Abbreviation = ?, Year_Founded = ?, League_ID = ?, College_ID = ?
                                WHERE Team_ID = ?
                            """, team_name, team_location, abbreviation, year_founded, league_id, college_id, team_id)
                            print(f"🔁 Updated team: {team_name} (ID {team_id})")
                        else:
                            cursor.execute("""
                                INSERT INTO Team (Team_ID, Team_Name, Team_Location, Abbreviation, Year_Founded, League_ID, College_ID)
                                VALUES (?, ?, ?, ?, ?, ?, ?)
                            """, team_id, team_name, team_location, abbreviation, year_founded, league_id, college_id)
                            print(f"✅ Inserted team: {team_name} (ID {team_id})")

                    except Exception as e:
                        print(f"❌ Error processing team {row.get('full_name', '[UNKNOWN]')}: {e}")

            if filename == "draft_history.csv":
                df = df.head(1)
                for _, row in df.iterrows():
                    try:
                        player_name = str(row.get("player_name", "")).strip()
                        college_name = str(row.get("organization", "")).strip()
                        team_name = str(row.get("team_name", "")).strip()
                        team_location = str(row.get("team_city", "")).strip()

                        # --- Handle College ---
                        cursor.execute("SELECT College_ID FROM College WHERE LOWER(College_Name) = LOWER(?)", college_name)
                        college_result = cursor.fetchone()

                        if college_result:
                            college_id = college_result[0]
                        else:
                            # Generate next College_ID
                            cursor.execute("SELECT ISNULL(MAX(College_ID), 0) + 1 FROM College")
                            college_id = cursor.fetchone()[0]

                            cursor.execute("""
                                INSERT INTO College (College_ID, College_Name)
                                VALUES (?, ?)
                            """, college_id, college_name)
                            print(f"✅ Inserted college: {college_name} (ID {college_id})")

                        # --- Handle Player ---
                        cursor.execute("SELECT Player_ID FROM Player WHERE LOWER(Player_Name) = LOWER(?)", player_name)
                        player_result = cursor.fetchone()

                        if player_result:
                            player_id = player_result[0]
                            cursor.execute("UPDATE Player SET College_ID = ? WHERE Player_ID = ?", college_id, player_id)
                            print(f"🔁 Linked college to player: {player_name} (ID {player_id})")
                        else:
                            cursor.execute("""
                                INSERT INTO Player (Player_Name, College_ID)
                                OUTPUT INSERTED.Player_ID
                                VALUES (?, ?)
                            """, player_name, college_id)
                            player_id = cursor.fetchone()[0]
                            print(f"✅ Inserted player: {player_name} (ID {player_id})")

                        # --- Handle Team ---
                        cursor.execute("SELECT Team_ID, College_ID FROM Team WHERE LOWER(Team_Name) = LOWER(?)", team_name)
                        team_result = cursor.fetchone()

                        if team_result:
                            team_id, existing_college_id = team_result
                            if existing_college_id != college_id:
                                # Link the existing team to the college
                                cursor.execute("UPDATE Team SET College_ID = ? WHERE Team_ID = ?", college_id, team_id)
                                print(f"🔁 Linked existing team '{team_name}' to college ID {college_id}")
                        else:
                            # Generate and insert new team linked to the college
                            cursor.execute("SELECT ISNULL(MAX(Team_ID), 0) + 1 FROM Team")
                            team_id = cursor.fetchone()[0]
                            cursor.execute("""
                                INSERT INTO Team (Team_ID, Team_Name, College_ID)
                                VALUES (?, ?, ?)
                            """, team_id, team_name, college_id)
                            print(f"✅ Inserted new team: {team_name} (ID {team_id}) linked to college ID {college_id}")

                    except Exception as e:
                        print(f"❌ Error processing row for player '{row.get('player_name')}': {e}")
            # --- Process game_summary.csv ---
            if filename == "game_summary.csv":
                for _, row in df.iterrows():
                    try:
                        game_id = int(row.get('game_id'))
                        home_team_id = int(row.get('home_team_id'))
                        visitor_team_id = int(row.get('visitor_team_id'))

                        # Fetch game score from Game table
                        cursor.execute("SELECT Game_Score FROM Game WHERE Game_ID = ?", game_id)
                        result = cursor.fetchone()
                        if not result:
                            print(f"⚠️ Skipping: Game {game_id} not found in Game table.")
                            continue

                        game_score = result[0]
                        if not game_score or "-" not in game_score:
                            print(f"⚠️ Invalid score format for Game {game_id}")
                            continue

                        home_score_str, away_score_str = game_score.split(' - ')
                        home_score = int(float(home_score_str.strip()))
                        away_score = int(float(away_score_str.strip()))

                        # Determine results
                        if home_score > away_score:
                            home_result, away_result = "Win", "Loss"
                        elif away_score > home_score:
                            home_result, away_result = "Loss", "Win"
                        else:
                            home_result = away_result = "Draw"

                        # Ensure both teams exist to avoid FK errors
                        cursor.execute("SELECT 1 FROM Team WHERE Team_ID = ?", home_team_id)
                        if not cursor.fetchone():
                            print(f"⚠️ Skipping: Home_Team_ID {home_team_id} not found for Game_ID {game_id}")
                            continue

                        cursor.execute("SELECT 1 FROM Team WHERE Team_ID = ?", visitor_team_id)
                        if not cursor.fetchone():
                            print(f"⚠️ Skipping: Visitor_Team_ID {visitor_team_id} not found for Game_ID {game_id}")
                            continue

                        # Insert or update home team attendance
                        cursor.execute("""
                            MERGE Game_Attendance AS target
                            USING (SELECT ? AS Game_ID, ? AS Team_ID) AS source
                            ON target.Game_ID = source.Game_ID AND target.Team_ID = source.Team_ID
                            WHEN MATCHED THEN
                                UPDATE SET Team_Score = ?, Result = ?, Home_or_Away = 'Home'
                            WHEN NOT MATCHED THEN
                                INSERT (Game_ID, Team_ID, Team_Score, Result, Home_or_Away)
                                VALUES (?, ?, ?, ?, 'Home');
                        """, game_id, home_team_id, home_score, home_result,
                             game_id, home_team_id, home_score, home_result)

                        # Insert or update visitor team attendance
                        cursor.execute("""
                            MERGE Game_Attendance AS target
                            USING (SELECT ? AS Game_ID, ? AS Team_ID) AS source
                            ON target.Game_ID = source.Game_ID AND target.Team_ID = source.Team_ID
                            WHEN MATCHED THEN
                                UPDATE SET Team_Score = ?, Result = ?, Home_or_Away = 'Away'
                            WHEN NOT MATCHED THEN
                                INSERT (Game_ID, Team_ID, Team_Score, Result, Home_or_Away)
                                VALUES (?, ?, ?, ?, 'Away');
                        """, game_id, visitor_team_id, away_score, away_result,
                             game_id, visitor_team_id, away_score, away_result)

                        print(f"✅ Linked game {game_id} to teams {home_team_id} (Home) and {visitor_team_id} (Away)")

                    except Exception as e:
                        print(f"❌ Error processing game summary row: {e}")




        except Exception as e:
            print(f"❌ Error loading file {filename}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print("🎉 All data processed successfully.")

# Run main
if __name__ == "__main__":
    main()
