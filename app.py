import streamlit as st
import pandas as pd
from datetime import datetime
import os

# --- CONFIGURATION ---
# Default to the sample database bundled with the repo. Swap this with a Google Sheets
# pull in production (see README for instructions).
DATABASE_FILE = 'Database.csv'
VEHICLE_LOG_FILE = 'Fuel_Log_Vehicles.csv'
TANKER_LOG_FILE = 'Fuel_Log_Tankers.csv'

# --- 1. LOAD DATA ---
@st.cache_data
def load_data():
    if not os.path.exists(DATABASE_FILE):
        st.error(f"⚠️ Critical Error: '{DATABASE_FILE}' not found. Please save the database CSV in this folder.")
        return pd.DataFrame()
    return pd.read_csv(DATABASE_FILE)

def load_logs(file_path, columns):
    if not os.path.exists(file_path):
        return pd.DataFrame(columns=columns)
    return pd.read_csv(file_path)

def save_log(df, file_path):
    df.to_csv(file_path, index=False)

# Load DB
db = load_data()

# --- 2. SIDEBAR NAVIGATION ---
st.set_page_config(page_title="Fuel Command Center", layout="wide", page_icon="⛽")

st.sidebar.title("⛽ Fuel Command Center")
page = st.sidebar.radio("Navigate", ["📝 Log Entry", "📊 Analytics Dashboard", "🛢️ Tanker Inventory"])

# --- 3. PAGE: LOG ENTRY (The Fast Entry System) ---
if page == "📝 Log Entry":
    st.title("New Fuel Transaction")

    if db.empty:
        st.warning("Database is empty. Please check Updated_Database.csv")
        st.stop()

    # A. Choose Operation Type
    operation_type = st.radio("Select Operation:", ["Dispense to Fleet (OUT)", "Refill Tanker (IN)"], horizontal=True)

    if operation_type == "Dispense to Fleet (OUT)":
        st.info("Log fuel dispensing from a Tanker to a Vehicle/Equipment.")

        col1, col2 = st.columns(2)

        with col1:
            # --- THE SMART SEARCH FEATURE ---
            # Create a label for searching: "HSC-116 | FUEL TANKER (DA 4247)"
            db['Search_Label'] = db['Fleet No'].astype(str) + " | " + db['Description'].astype(str) + " (" + db['Plate Number'].astype(str) + ")"

            # The Selectbox
            selected_label = st.selectbox("🔍 Search Fleet No (Type to Search):", options=[""] + list(db['Search_Label'].unique()))

            # Auto-fill logic
            fleet_no = None
            category = None
            asset_row = None

            if selected_label:
                # Find the row in the DB
                asset_row = db[db['Search_Label'] == selected_label].iloc[0]

                st.success(f"**Selected:** {asset_row['Description']}")

                # Display details in a clean format
                d_col1, d_col2 = st.columns(2)
                d_col1.write(f"**Category:** {asset_row['Category']}")
                d_col2.write(f"**Plate:** {asset_row['Plate Number']}")

                fleet_no = asset_row['Fleet No']
                category = asset_row['Category']

        with col2:
            # Source Tanker Selection
            tankers = db[db['Category'] == 'Tanker']['Fleet No'].tolist()
            # Fallback if no tankers categorized
            if not tankers:
                tankers = ['BPS-95', 'HSC-116', 'BPS-13', 'HSC-101']

            source_tanker = st.selectbox("⛽ Source Tanker (Dispenser):", options=tankers)

            # Inputs
            date = st.date_input("Date", datetime.today())
            fuel_qty = st.number_input("Fuel Dispensed (Liters)", min_value=1.0, step=1.0)

            # Meter Logic based on Category
            meter_unit = "Km"
            if category and category in ['Equipment', 'Machine', 'Tanker']:
                meter_unit = "Hours"

            current_meter = st.number_input(f"Current Odometer/Hour Meter ({meter_unit})", min_value=0.0, step=1.0)

        # Validation & Submission
        if st.button("Submit Entry", type="primary"):
            if not fleet_no:
                st.error("Please select a Fleet Number.")
            else:
                new_entry = {
                    "Date": date,
                    "Fleet No": fleet_no,
                    "Asset ID": asset_row['Asset ID'],
                    "Category": category,
                    "Description": asset_row['Description'],
                    "Source Tanker": source_tanker,
                    "Fuel Out (L)": fuel_qty,
                    "Current Meter": current_meter,
                    "Meter Unit": meter_unit
                }

                # Save to Local CSV (Simulating Google Sheet Append)
                log_df = load_logs(VEHICLE_LOG_FILE, new_entry.keys())
                new_df = pd.DataFrame([new_entry])
                log_df = pd.concat([log_df, new_df], ignore_index=True)
                save_log(log_df, VEHICLE_LOG_FILE)

                st.toast(f"Logged {fuel_qty}L for {fleet_no}!")
                st.success(f"✅ Transaction Saved. {fleet_no} took {fuel_qty}L from {source_tanker}.")

    elif operation_type == "Refill Tanker (IN)":
        st.warning("Log fuel COMING IN to your Tankers from External Stations.")

        col1, col2 = st.columns(2)
        with col1:
            tankers = db[db['Category'] == 'Tanker']['Fleet No'].tolist()
            if not tankers:
                tankers = ['BPS-95', 'HSC-116', 'BPS-13', 'HSC-101']
            target_tanker = st.selectbox("Select Tanker Receiving Fuel:", options=tankers)

        with col2:
            source_station = st.text_input("External Station Name (e.g., Shell Haima):")

        vol_in = st.number_input("Volume Received (Liters):", min_value=1)
        date_in = st.date_input("Date", datetime.today())

        if st.button("Log Refill"):
            entry = {
                "Date": date_in,
                "Tanker No": target_tanker,
                "Source Station": source_station,
                "Fuel In (L)": vol_in
            }
            log_df = load_logs(TANKER_LOG_FILE, entry.keys())
            new_df = pd.DataFrame([entry])
            log_df = pd.concat([log_df, new_df], ignore_index=True)
            save_log(log_df, TANKER_LOG_FILE)

            st.success(f"✅ Added {vol_in}L to {target_tanker} Inventory.")

# --- 4. PAGE: DASHBOARD ---
elif page == "📊 Analytics Dashboard":
    st.title("Fuel Analytics")

    # Load Logs
    v_log = load_logs(VEHICLE_LOG_FILE, ["Date", "Fleet No", "Category", "Fuel Out (L)"])

    if v_log.empty:
        st.info("No data logged yet. Go to 'Log Entry' to start.")
    else:
        # KPI Row
        kpi1, kpi2, kpi3 = st.columns(3)
        total_fuel = v_log['Fuel Out (L)'].sum()
        total_entries = len(v_log)

        kpi1.metric("Total Fuel Consumed", f"{total_fuel:,.0f} L")
        kpi2.metric("Total Transactions", total_entries)
        kpi3.metric("Active Assets", v_log['Fleet No'].nunique())

        st.markdown("---")

        # Charts
        c1, c2 = st.columns(2)

        with c1:
            st.subheader("Consumption by Category")
            if not v_log.empty:
                chart_data = v_log.groupby('Category')['Fuel Out (L)'].sum()
                st.bar_chart(chart_data)

        with c2:
            st.subheader("Top Consumers (Vehicles)")
            if not v_log.empty:
                top_consumers = v_log.groupby('Fleet No')['Fuel Out (L)'].sum().sort_values(ascending=False).head(5)
                st.bar_chart(top_consumers)

        st.subheader("Recent Transactions")
        st.dataframe(v_log.sort_index(ascending=False).head(10), use_container_width=True)

# --- 5. PAGE: TANKER INVENTORY ---
elif page == "🛢️ Tanker Inventory":
    st.title("Tanker Balances")
    st.write("Live tracking of fuel inside your 4 mobile tankers.")

    # Load Data
    t_log = load_logs(TANKER_LOG_FILE, ["Tanker No", "Fuel In (L)"])
    v_log = load_logs(VEHICLE_LOG_FILE, ["Source Tanker", "Fuel Out (L)"])

    tankers = ['BPS-95', 'HSC-116', 'BPS-13', 'HSC-101']

    col_grid = st.columns(2)

    for i, t in enumerate(tankers):
        # Calculate Logic
        # 1. Total Refills (IN)
        if not t_log.empty:
            total_in = t_log[t_log['Tanker No'] == t]['Fuel In (L)'].sum()
        else:
            total_in = 0

        # 2. Total Dispensed (OUT)
        if not v_log.empty:
            total_out = v_log[v_log['Source Tanker'] == t]['Fuel Out (L)'].sum()
        else:
            total_out = 0

        # 3. Current Balance
        # Assuming a starting balance of 0 for now (or you can add a 'Initial Balance' feature)
        current_balance = total_in - total_out

        with col_grid[i % 2]:
            st.container(border=True)
            st.subheader(f"🚛 {t}")
            st.metric("Current Level", f"{current_balance:,.0f} L")

            # Simple bar chart for tank level (Assuming 30,000L Capacity)
            capacity = 30000
            percent = max(0.0, min(1.0, current_balance / capacity))
            st.progress(percent)
            st.caption(f"IN: {total_in} L | OUT: {total_out} L")
