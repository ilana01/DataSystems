import streamlit as st
import pyodbc
import pandas as pd
import datetime


# --- login credentials and roles ---
USER_CREDENTIALS = {
    "admin@datadraft.com": {"password": "pass123", "role": "admin"},
    "scout@scout.com": {"password": "scout2025", "role": "scout"},
}

# --- SQL Server connection string ---
server = 'datadraft.database.windows.net'
database = 'DataDraft_Database'
username = 'datadraft'
password = 'Datasystems2025'
driver = '{ODBC Driver 18 for SQL Server}'
conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'

# --- Fetch data ---
@st.cache_data
def fetch_data(table):
    conn = pyodbc.connect(conn_str)
    query = f"SELECT * FROM {table}"
    df = pd.read_sql(query, conn)
    conn.close()
    return df
def validate_user(username, password):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT Role, User_F_Name, User_L_Name FROM [User] WHERE Email = ? AND User_Password = ?",
        username, password
    )
    result = cursor.fetchone()
    conn.close()
    if result:
        role, fname, lname = result
        return {"role": role, "full_name": f"{fname} {lname}"}
    return None

def register_user(fname, lname, password, email, phone, dob, role="scout"):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO [User] (User_Password, User_F_Name, User_L_Name, Role, Email, Phone_Number, DOB)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            password, fname, lname, role, email, phone, dob
        )
        conn.commit()
        return True
    except pyodbc.IntegrityError:
        return False
    finally:
        conn.close()


# --- Login Page ---
def login():
    st.title("🔐 Login / Register")

    tab1, tab2 = st.tabs(["🔑 Login", "📝 Register"])

    # --- Login Tab ---
    with tab1:
        email = st.text_input("Email", key="login_email")
        password = st.text_input("Password", type="password", key="login_pass")
        login_button = st.button("Login")

        if login_button:
            user = validate_user(email, password)
            if user:
                st.session_state.logged_in = True
                st.session_state.username = email
                st.session_state.role = user["role"]
                st.session_state.full_name = user["full_name"]
                st.rerun()
            else:
                st.error("Invalid email or password.")

    # --- Register Tab ---
    with tab2:
        fname = st.text_input("First Name")
        lname = st.text_input("Last Name")
        email = st.text_input("Email")
        password = st.text_input("Password", type="password")
        confirm_password = st.text_input("Confirm Password", type="password")
        phone = st.text_input("Phone Number")
        dob = st.date_input(
            "Date of Birth",
            min_value=datetime.date(1900, 1, 1),
            max_value=datetime.date.today()
        )
        role = st.selectbox("Role", ["scout", "admin"])
        register_button = st.button("Register")

        if register_button:
            if password != confirm_password:
                st.error("Passwords do not match.")
            elif not all([fname, lname, email, password]):
                st.warning("Please fill out all required fields.")
            else:
                success = register_user(fname, lname, password, email, phone, dob, role)
                if success:
                    st.success("Registration successful! You can now log in.")
                else:
                    st.error("User with this email already exists or registration failed.")

# --- Main App ---
def main_app():
    role = st.session_state.get("role")
    username = st.session_state.get("username")
    st.title("🏀 Data Viewer")
    username = st.session_state.get("username")
    full_name = st.session_state.get("full_name")
    role = st.session_state.get("role")

    st.success(f"Logged in as {full_name} ({role})")

    # Show different data depending on role
    if role == "admin":
        tab1, tab2, tab3 = st.tabs(["🧍 Player View", "🏀 Team View", "📊 Compare Players"])

        # --- Player Tab ---
        with tab1:
            st.subheader("🔍 Player Search with Filters")

            # Fetch data
            player_df = fetch_data("Player")
            perf_df = fetch_data("Performance_Statistics")
            team_df = fetch_data("Team")
            college_df = fetch_data("College")
            league_df = fetch_data("League")

            # Merge team and college names into player_df
            player_df = player_df.merge(team_df[["Team_ID", "Team_Name"]], on="Team_ID", how="left")
            player_df = player_df.merge(college_df[["College_ID", "College_Name"]], on="College_ID", how="left")

            # --- Filters ---
            st.markdown("### 📌 Filters")

            min_height = int(player_df["Height"].min())
            max_height = int(player_df["Height"].max())
            height_range = st.slider("Height (inches)", min_value=min_height, max_value=max_height, value=(min_height, max_height))

            nationalities = sorted(player_df["Nationality"].dropna().unique())
            nationality_filter = st.selectbox("Nationality", ["All"] + nationalities)

            team_names = sorted(player_df["Team_Name"].dropna().unique())
            team_filter = st.selectbox("Team", ["All"] + team_names)

            college_names = sorted(player_df["College_Name"].dropna().unique())
            college_filter = st.selectbox("College", ["All"] + college_names)

            # Apply filters
            filtered_df = player_df[
                (player_df["Height"] >= height_range[0]) & (player_df["Height"] <= height_range[1])
            ]

            if nationality_filter != "All":
                filtered_df = filtered_df[filtered_df["Nationality"] == nationality_filter]
            if team_filter != "All":
                filtered_df = filtered_df[filtered_df["Team_Name"] == team_filter]
            if college_filter != "All":
                filtered_df = filtered_df[filtered_df["College_Name"] == college_filter]

            player_names = filtered_df["Player_Name"].dropna().sort_values().unique()
            selected_name = st.selectbox("Select a Player", player_names if len(player_names) > 0 else ["No players found"])

            if selected_name and selected_name != "No players found":
                selected_player = player_df[player_df["Player_Name"] == selected_name].iloc[0]

                st.markdown(f"### 🧍 Player Details: {selected_name}")
                st.write({
                    "Nationality": selected_player["Nationality"],
                    "Height": selected_player["Height"],
                    "Weight": selected_player["Weight"],
                    "Current Salary": selected_player["Current_Salary"],
                    "Team": selected_player["Team_Name"],
                    "Rating": selected_player["Rating"],
                    "College": selected_player["College_Name"],
                })

                st.markdown("### 📈 Performance Statistics")
                stats = perf_df[perf_df["Player_ID"] == selected_player["Player_ID"]]
                if not stats.empty:
                    avg_stats = stats.drop(columns=["Player_ID", "Performance_ID", "Season"]).mean(numeric_only=True)
                    st.dataframe(avg_stats.to_frame("Average").T, use_container_width=True)
                else:
                    st.info("No performance statistics found for this player.")

        # --- Team Tab ---
        with tab2:
            st.subheader("🏀 Team Table")
            df = fetch_data("Team")
            league_df = fetch_data("League")
            college_df = fetch_data("College")

            # Merge League and College names
            df = df.merge(league_df[["League_ID", "League_Name"]], on="League_ID", how="left")
            df = df.merge(college_df[["College_ID", "College_Name"]], on="College_ID", how="left")

            # Optional: drop raw ID columns for clarity
            df.drop(columns=["League_ID", "College_ID"], inplace=True, errors="ignore")

            st.dataframe(df, use_container_width=True)

            if "Team_Location" in df.columns:
                st.subheader("📍 Team Count by Location")
                location_counts = df["Team_Location"].value_counts().reset_index()
                location_counts.columns = ["Location", "Team Count"]
                st.bar_chart(location_counts.set_index("Location"))

            if "League_Name" in df.columns:
                st.subheader("🏆 Teams per League")
                league_counts = df["League_Name"].value_counts().reset_index()
                league_counts.columns = ["League Name", "Team Count"]
                st.bar_chart(league_counts.set_index("League Name"))

            if "Year_Founded" in df.columns:
                st.subheader("📅 Teams by Founding Year")
                founded_counts = df["Year_Founded"].dropna().astype(int).value_counts().sort_index()
                st.line_chart(founded_counts)

            if "College_Name" in df.columns:
                st.subheader("🎓 College Affiliation")
                affiliated = df["College_Name"].notnull().sum()
                not_affiliated = df["College_Name"].isnull().sum()
                college_data = pd.DataFrame({
                    "Type": ["Affiliated", "Not Affiliated"],
                    "Count": [affiliated, not_affiliated]
                })
                st.bar_chart(college_data.set_index("Type"))

        # --- Compare Players Tab ---
        with tab3:
            st.subheader("📊 Compare Players")

            player_df = fetch_data("Player")
            perf_df = fetch_data("Performance_Statistics")
            team_df = fetch_data("Team")
            college_df = fetch_data("College")

            # Enrich player_df with team and college names
            player_df = player_df.merge(team_df[["Team_ID", "Team_Name"]], on="Team_ID", how="left")
            player_df = player_df.merge(college_df[["College_ID", "College_Name"]], on="College_ID", how="left")

            player_names = player_df["Player_Name"].dropna().sort_values().unique()

            col1, col2 = st.columns(2)
            with col1:
                player1_name = st.selectbox("Select Player 1", player_names, key="player1")
            with col2:
                player2_name = st.selectbox("Select Player 2", player_names, key="player2")

            if player1_name and player2_name and player1_name != player2_name:
                player1 = player_df[player_df["Player_Name"] == player1_name].iloc[0]
                player2 = player_df[player_df["Player_Name"] == player2_name].iloc[0]

                # --- Display Basic Info Side by Side ---
                st.markdown("### 🔍 Basic Information")
                basic_info = pd.DataFrame({
                    "Attribute": ["Nationality", "Height", "Weight", "Salary", "Team", "Rating", "College"],
                    player1_name: [
                        player1["Nationality"], player1["Height"], player1["Weight"],
                        player1["Current_Salary"], player1["Team_Name"], player1["Rating"], player1["College_Name"]
                    ],
                    player2_name: [
                        player2["Nationality"], player2["Height"], player2["Weight"],
                        player2["Current_Salary"], player2["Team_Name"], player2["Rating"], player2["College_Name"]
                    ]
                })
                st.dataframe(basic_info, use_container_width=True)

                # --- Display Average Performance Stats ---
                st.markdown("### 📈 Average Performance Statistics")
                stats1 = perf_df[perf_df["Player_ID"] == player1["Player_ID"]]
                stats2 = perf_df[perf_df["Player_ID"] == player2["Player_ID"]]

                if not stats1.empty and not stats2.empty:
                    avg1 = stats1.drop(columns=["Player_ID", "Performance_ID", "Season"]).mean(numeric_only=True)
                    avg2 = stats2.drop(columns=["Player_ID", "Performance_ID", "Season"]).mean(numeric_only=True)

                    comparison_df = pd.DataFrame({
                        "Stat": avg1.index,
                        player1_name: avg1.values,
                        player2_name: avg2.values
                    })

                    st.dataframe(comparison_df, use_container_width=True)
                else:
                    st.info("No performance data found for one or both players.")
            elif player1_name == player2_name:
                st.warning("Please select two different players to compare.")


    else:
        st.subheader("🏀 Team Table")
        df = fetch_data("Team")
        league_df = fetch_data("League")
        college_df = fetch_data("College")

        df = df.merge(league_df[["League_ID", "League_Name"]], on="League_ID", how="left")
        df = df.merge(college_df[["College_ID", "College_Name"]], on="College_ID", how="left")
        df.drop(columns=["League_ID", "College_ID"], inplace=True, errors="ignore")

        st.dataframe(df, use_container_width=True)

        if "Team_Location" in df.columns:
            st.subheader("📍 Team Count by Location")
            location_counts = df["Team_Location"].value_counts().reset_index()
            location_counts.columns = ["Location", "Team Count"]
            st.bar_chart(location_counts.set_index("Location"))

        if "League_Name" in df.columns:
            st.subheader("🏆 Teams per League")
            league_counts = df["League_Name"].value_counts().reset_index()
            league_counts.columns = ["League Name", "Team Count"]
            st.bar_chart(league_counts.set_index("League Name"))

        if "Year_Founded" in df.columns:
            st.subheader("📅 Teams by Founding Year")
            founded_counts = df["Year_Founded"].dropna().astype(int).value_counts().sort_index()
            st.line_chart(founded_counts)

        if "College_Name" in df.columns:
            st.subheader("🎓 College Affiliation")
            affiliated = df["College_Name"].notnull().sum()
            not_affiliated = df["College_Name"].isnull().sum()
            college_data = pd.DataFrame({
                "Type": ["Affiliated", "Not Affiliated"],
                "Count": [affiliated, not_affiliated]
            })
            st.bar_chart(college_data.set_index("Type"))


    if st.button("Logout"):
        st.session_state.logged_in = False
        st.rerun()

# --- App Launcher ---
def main():
    if "logged_in" not in st.session_state:
        st.session_state.logged_in = False

    if st.session_state.logged_in:
        main_app()
    else:
        login()

if __name__ == "__main__":
    main()