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
        st.subheader("Player Table")
        df = fetch_data("Player")
        st.dataframe(df, use_container_width=True)
    elif role == "scout":
        st.subheader("Team Table")
        df = fetch_data("Team")
        st.dataframe(df, use_container_width=True)
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
