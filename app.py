from datetime import datetime

import gspread
import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="Fuel Command Center", layout="wide", page_icon="⛽")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

DISPENSE_HEADER = [
    "Timestamp",
    "Date",
    "Fleet No",
    "Asset ID",
    "Category",
    "Description",
    "Source Tanker",
    "Fuel Out (L)",
    "Current Meter",
    "Meter Unit",
]

RECEIPT_HEADER = ["Timestamp", "Date", "Tanker No", "Source Station", "Fuel In (L)"]

ALLOWED_CATEGORIES = ["Vehicle", "Bus", "Equipment", "Machine", "Tanker"]
CATEGORY_ALIASES = {
    "vehicle": "Vehicle",
    "vehicles": "Vehicle",
    "bus": "Bus",
    "buses": "Bus",
    "equipment": "Equipment",
    "machine": "Machine",
    "machines": "Machine",
    "machine/equipment": "Equipment",
    "equipment/machine": "Equipment",
    "tanker": "Tanker",
    "tankers": "Tanker",
}
DEFAULT_TANKERS = ["BPS-95", "HSC-116", "BPS-13", "HSC-101"]
METER_HOUR_CATEGORIES = {"Equipment", "Machine", "Tanker"}


@st.cache_resource
def get_gs_client():
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"], scopes=SCOPES
    )
    return gspread.authorize(creds)


@st.cache_resource
def get_spreadsheet():
    return get_gs_client().open_by_url(st.secrets["sheet_url"])


def get_worksheet(spreadsheet, title: str):
    try:
        return spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        return spreadsheet.add_worksheet(title=title, rows=1000, cols=20)


def ws_to_df(worksheet) -> pd.DataFrame:
    records = worksheet.get_all_records()
    return pd.DataFrame(records) if records else pd.DataFrame()


def ensure_header(worksheet, header):
    current = worksheet.row_values(1)
    if current != header:
        worksheet.clear()
        worksheet.append_row(header, value_input_option="RAW")


def append_dict(worksheet, row_dict, header):
    ensure_header(worksheet, header)
    worksheet.append_row(
        [row_dict.get(h, "") for h in header], value_input_option="USER_ENTERED"
    )


def normalize_category(category: str) -> str:
    if pd.isna(category):
        return ""

    raw_value = str(category).strip()
    normalized_key = raw_value.replace("&", "/").replace(" and ", "/").lower()
    normalized_key = normalized_key.replace(" ", "")

    canonical = CATEGORY_ALIASES.get(normalized_key)
    if canonical:
        return canonical

    if raw_value.title() in ALLOWED_CATEGORIES:
        return raw_value.title()

    return raw_value


def build_search_labels(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe

    return dataframe.assign(
        Search_Label=lambda frame: (
            frame["Fleet No"].astype(str)
            + " | "
            + frame["Description"].astype(str)
            + " ("
            + frame["Plate Number"].astype(str)
            + ")"
        )
    )


def get_tanker_options(dataframe: pd.DataFrame):
    tankers = dataframe[dataframe["Category"] == "Tanker"]["Fleet No"].tolist()

    if tankers:
        return tankers

    return DEFAULT_TANKERS


def get_worksheets():
    spreadsheet = get_spreadsheet()
    return (
        get_worksheet(spreadsheet, "Assets"),
        get_worksheet(spreadsheet, "Tanker Dispensing"),
        get_worksheet(spreadsheet, "Tanker Receipts"),
    )


def require_secrets():
    missing = [
        key for key in ("gcp_service_account", "sheet_url") if key not in st.secrets
    ]

    if missing:
        st.error(
            "Missing Streamlit secrets: " + ", ".join(missing) + ". "
            "Please set them in .streamlit/secrets.toml."
        )
        st.stop()


require_secrets()
ws_assets, ws_dispense, ws_receipts = get_worksheets()

DATABASE = ws_to_df(ws_assets)
if "Category" in DATABASE.columns:
    DATABASE["Category"] = DATABASE["Category"].apply(normalize_category)

st.sidebar.title("⛽ Fuel Command Center")
st.sidebar.caption("Fast data entry and live analytics for tanker operations.")
PAGE = st.sidebar.radio(
    "Navigate", ["📝 Log Entry", "📊 Analytics Dashboard", "🛢️ Tanker Inventory"]
)

if PAGE == "📝 Log Entry":
    st.title("New Fuel Transaction")

    if DATABASE.empty:
        st.warning("Database is empty. Please check the 'Assets' tab in Google Sheets.")
        st.stop()

    operation_type = st.radio(
        "Select Operation:", ["Dispense to Fleet (OUT)", "Refill Tanker (IN)"], horizontal=True
    )

    if operation_type == "Dispense to Fleet (OUT)":
        st.info("Log fuel dispensing from a Tanker to a Vehicle/Equipment.")

        column_left, column_right = st.columns(2)

        with column_left:
            searchable_db = build_search_labels(DATABASE)
            search_options = [""] + list(searchable_db["Search_Label"].unique())
            selected_label = st.selectbox(
                "🔍 Search Fleet No (Type to Search):",
                options=search_options,
            )

            fleet_no = None
            category = None
            asset_row = None

            if selected_label:
                asset_row = searchable_db[searchable_db["Search_Label"] == selected_label].iloc[0]

                st.success(f"**Selected:** {asset_row['Description']}")

                detail_column_1, detail_column_2 = st.columns(2)
                detail_column_1.write(f"**Category:** {asset_row['Category']}")
                detail_column_2.write(f"**Plate:** {asset_row['Plate Number']}")

                fleet_no = asset_row["Fleet No"]
                category = asset_row["Category"]

        with column_right:
            tanker_options = get_tanker_options(DATABASE)
            source_tanker = st.selectbox("⛽ Source Tanker (Dispenser):", options=tanker_options)

            date = st.date_input("Date", datetime.today())
            fuel_qty = st.number_input("Fuel Dispensed (Liters)", min_value=1.0, step=1.0)

            meter_unit = "Km"
            if category and category in METER_HOUR_CATEGORIES:
                meter_unit = "Hours"

            current_meter = st.number_input(
                f"Current Odometer/Hour Meter ({meter_unit})", min_value=0.0, step=1.0
            )

        if st.button("Submit Entry", type="primary"):
            if not fleet_no or asset_row is None:
                st.error("Please select a Fleet Number.")
            else:
                new_entry = {
                    "Timestamp": datetime.utcnow().isoformat(),
                    "Date": str(date),
                    "Fleet No": fleet_no,
                    "Asset ID": asset_row.get("Asset ID", ""),
                    "Category": category,
                    "Description": asset_row.get("Description", ""),
                    "Source Tanker": source_tanker,
                    "Fuel Out (L)": fuel_qty,
                    "Current Meter": current_meter,
                    "Meter Unit": meter_unit,
                }

                append_dict(ws_dispense, new_entry, DISPENSE_HEADER)

                st.toast(f"Logged {fuel_qty}L for {fleet_no}!")
                st.success(
                    f"✅ Transaction Saved. {fleet_no} took {fuel_qty}L from {source_tanker}."
                )

    elif operation_type == "Refill Tanker (IN)":
        st.warning("Log fuel COMING IN to your Tankers from External Stations.")

        column_left, column_right = st.columns(2)
        with column_left:
            tanker_options = get_tanker_options(DATABASE)
            target_tanker = st.selectbox("Select Tanker Receiving Fuel:", options=tanker_options)

        with column_right:
            source_station = st.text_input("External Station Name (e.g., Shell Haima):")

        vol_in = st.number_input("Volume Received (Liters):", min_value=1)
        date_in = st.date_input("Date", datetime.today())

        if st.button("Log Refill"):
            entry = {
                "Timestamp": datetime.utcnow().isoformat(),
                "Date": str(date_in),
                "Tanker No": target_tanker,
                "Source Station": source_station,
                "Fuel In (L)": vol_in,
            }
            append_dict(ws_receipts, entry, RECEIPT_HEADER)

            st.success(f"✅ Added {vol_in}L to {target_tanker} Inventory.")

elif PAGE == "📊 Analytics Dashboard":
    st.title("Fuel Analytics")

    vehicle_log = ws_to_df(ws_dispense)

    if vehicle_log.empty:
        st.info("No data logged yet. Go to 'Log Entry' to start.")
    else:
        vehicle_log["Fuel Out (L)"] = pd.to_numeric(
            vehicle_log.get("Fuel Out (L)"), errors="coerce"
        ).fillna(0)

        kpi1, kpi2, kpi3 = st.columns(3)
        total_fuel = vehicle_log["Fuel Out (L)"].sum()
        total_entries = len(vehicle_log)

        kpi1.metric("Total Fuel Consumed", f"{total_fuel:,.0f} L")
        kpi2.metric("Total Transactions", total_entries)
        kpi3.metric("Active Assets", vehicle_log["Fleet No"].nunique())

        st.markdown("---")

        chart_column_1, chart_column_2 = st.columns(2)

        with chart_column_1:
            st.subheader("Consumption by Category")
            chart_data = vehicle_log.groupby("Category")["Fuel Out (L)"].sum()
            st.bar_chart(chart_data)

        with chart_column_2:
            st.subheader("Top Consumers (Vehicles)")
            top_consumers = (
                vehicle_log.groupby("Fleet No")["Fuel Out (L)"]
                .sum()
                .sort_values(ascending=False)
                .head(5)
            )
            st.bar_chart(top_consumers)

        st.subheader("Recent Transactions")
        st.dataframe(
            vehicle_log.sort_index(ascending=False).head(10), use_container_width=True
        )

elif PAGE == "🛢️ Tanker Inventory":
    st.title("Tanker Balances")
    st.write("Live tracking of fuel inside your 4 mobile tankers.")

    receipts_log = ws_to_df(ws_receipts)
    vehicle_log = ws_to_df(ws_dispense)

    if not receipts_log.empty:
        receipts_log["Fuel In (L)"] = pd.to_numeric(
            receipts_log.get("Fuel In (L)"), errors="coerce"
        ).fillna(0)

    if not vehicle_log.empty:
        vehicle_log["Fuel Out (L)"] = pd.to_numeric(
            vehicle_log.get("Fuel Out (L)"), errors="coerce"
        ).fillna(0)

    column_grid = st.columns(2)

    for index, tanker in enumerate(DEFAULT_TANKERS):
        total_in = 0
        total_out = 0

        if not receipts_log.empty and "Tanker No" in receipts_log.columns:
            total_in = receipts_log[receipts_log["Tanker No"] == tanker]["Fuel In (L)"].sum()

        if not vehicle_log.empty and "Source Tanker" in vehicle_log.columns:
            total_out = (
                vehicle_log[vehicle_log["Source Tanker"] == tanker]["Fuel Out (L)"].sum()
            )

        current_balance = total_in - total_out

        with column_grid[index % 2]:
            st.container(border=True)
            st.subheader(f"🚛 {tanker}")
            st.metric("Current Level", f"{current_balance:,.0f} L")

            capacity = 30000
            percent = max(0.0, min(1.0, current_balance / capacity))
            st.progress(percent)
            st.caption(f"IN: {total_in} L | OUT: {total_out} L")
