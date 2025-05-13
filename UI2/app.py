import streamlit as st
import pyodbc
import pandas as pd
# --- Hardcoded login credentials and roles ---
USER_CREDENTIALS = {
    "admin": {"password": "pass123", "role": "admin"},
    "scout": {"password": "scout2025", "role": "scout"},
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

# --- Login Page ---
def login():
    st.title("🔐 Login")
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")
    login_button = st.button("Login")

    if login_button:
        user = USER_CREDENTIALS.get(username)
        if user and user["password"] == password:
            st.session_state.logged_in = True
            st.session_state.username = username
            st.session_state.role = user["role"]
            st.rerun()
        else:
            st.error("Invalid username or password.")

# --- Main App ---
def main_app():
    role = st.session_state.get("role")
    username = st.session_state.get("username")
    st.title("🏀 Data Viewer")
    st.success(f"Logged in as {username} ({role})")

    # Show different data depending on role
    if role == "admin":
        tab1, tab2 = st.tabs(["🧍 Player View", "🏀 Team View"])

        # --- Player Tab ---
        with tab1:
            st.subheader("🔍 Player Search with Filters")

            # Fetch data
            player_df = fetch_data("Player")
            perf_df = fetch_data("Performance_Statistics")
            team_df = fetch_data("Team")
            college_df = fetch_data("College")

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
            st.dataframe(df, use_container_width=True)

            if "Team_Location" in df.columns:
                st.subheader("📍 Team Count by Location")
                location_counts = df["Team_Location"].value_counts().reset_index()
                location_counts.columns = ["Location", "Team Count"]
                st.bar_chart(location_counts.set_index("Location"))

            if "League_ID" in df.columns:
                st.subheader("🏆 Teams per League")
                league_counts = df["League_ID"].value_counts().reset_index()
                league_counts.columns = ["League ID", "Team Count"]
                st.bar_chart(league_counts.set_index("League ID"))

            if "Year_Founded" in df.columns:
                st.subheader("📅 Teams by Founding Year")
                founded_counts = df["Year_Founded"].dropna().astype(int).value_counts().sort_index()
                st.line_chart(founded_counts)

            if "College_ID" in df.columns:
                st.subheader("🎓 College Affiliation")
                affiliated = df["College_ID"].notnull().sum()
                not_affiliated = df["College_ID"].isnull().sum()
                college_data = pd.DataFrame({
                    "Type": ["Affiliated", "Not Affiliated"],
                    "Count": [affiliated, not_affiliated]
                })
                st.bar_chart(college_data.set_index("Type"))

    elif role == "scout":
        st.subheader("🏀 Team Table")
        df = fetch_data("Team")
        st.dataframe(df, use_container_width=True)

        if "Team_Location" in df.columns:
            st.subheader("📍 Team Count by Location")
            location_counts = df["Team_Location"].value_counts().reset_index()
            location_counts.columns = ["Location", "Team Count"]
            st.bar_chart(location_counts.set_index("Location"))

        if "League_ID" in df.columns:
            st.subheader("🏆 Teams per League")
            league_counts = df["League_ID"].value_counts().reset_index()
            league_counts.columns = ["League ID", "Team Count"]
            st.bar_chart(league_counts.set_index("League ID"))

        if "Year_Founded" in df.columns:
            st.subheader("📅 Teams by Founding Year")
            founded_counts = df["Year_Founded"].dropna().astype(int).value_counts().sort_index()
            st.line_chart(founded_counts)

        if "College_ID" in df.columns:
            st.subheader("🎓 College Affiliation")
            affiliated = df["College_ID"].notnull().sum()
            not_affiliated = df["College_ID"].isnull().sum()
            college_data = pd.DataFrame({
                "Type": ["Affiliated", "Not Affiliated"],
                "Count": [affiliated, not_affiliated]
            })
            st.bar_chart(college_data.set_index("Type"))


    else:
        st.warning("No view configured for your role.")

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