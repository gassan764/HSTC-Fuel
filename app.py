import json
import logging
from datetime import datetime
from pathlib import Path

import gspread
import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

st.set_page_config(page_title="Fuel Command Center", layout="wide", page_icon="⛽")

BASE_DIR = Path(__file__).parent.resolve()
DATA_DIR = BASE_DIR / "data"
OUTPUTS_DIR = BASE_DIR / "outputs"

DATABASE_FILE = DATA_DIR / "Database.csv"
VEHICLE_LOG_FILE = OUTPUTS_DIR / "Fuel_Log_Vehicles.csv"
TANKER_LOG_FILE = OUTPUTS_DIR / "Fuel_Log_Tankers.csv"

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
GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
DISPENSING_HEADERS = [
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
RECEIPT_HEADERS = ["Timestamp", "Date", "Tanker No", "Source Station", "Fuel In (L)"]


class MissingSecretError(Exception):
    """Raised when required Streamlit secrets are absent."""


def ensure_directories() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)


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


@st.cache_data
def load_data(database_path: Path) -> pd.DataFrame:
    if not database_path.exists():
        st.error(
            f"⚠️ Critical Error: '{database_path.name}' not found. "
            "Please place the file in the data/ folder."
        )
        return pd.DataFrame()

    dataframe = pd.read_csv(database_path)

    if "Category" in dataframe.columns:
        dataframe["Category"] = dataframe["Category"].apply(normalize_category)

    return dataframe


def load_logs(file_path: Path, columns) -> pd.DataFrame:
    if not file_path.exists():
        return pd.DataFrame(columns=columns)

    return pd.read_csv(file_path)


def save_log(df: pd.DataFrame, file_path: Path) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(file_path, index=False)


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


@st.cache_resource
def get_gspread_client(service_account_info: dict) -> gspread.Client:
    credentials = Credentials.from_service_account_info(
        service_account_info, scopes=GOOGLE_SCOPES
    )
    return gspread.authorize(credentials)


@st.cache_resource
def get_spreadsheet(sheet_url: str, service_account_info: dict) -> gspread.Spreadsheet:
    client = get_gspread_client(service_account_info)
    return client.open_by_url(sheet_url)


def require_google_sheet():
    """
    Validate and return Google Sheets client resources.

    Important: call this only after the page has begun rendering to avoid
    accessing st.secrets at module import time. Streamlit Cloud mounts secrets
    at runtime, so early access can prevent the UI from rendering diagnostics.
    """

    secrets_keys = list(st.secrets.keys())
    sheet_url = st.secrets.get("sheet_url")
    service_account_info = st.secrets.get("gcp_service_account")

    if not sheet_url:
        raise MissingSecretError(
            f"Missing 'sheet_url' secret. Visible keys: {secrets_keys}"
        )

    if not service_account_info:
        raise MissingSecretError(
            f"Missing 'gcp_service_account' secret. Visible keys: {secrets_keys}"
        )

    # Normalize to a plain dict and stable JSON for caching keys
    normalized_service_account = json.loads(json.dumps(service_account_info))
    spreadsheet = get_spreadsheet(sheet_url, normalized_service_account)
    service_account_json = json.dumps(normalized_service_account, sort_keys=True)

    return spreadsheet, sheet_url, normalized_service_account, service_account_json, secrets_keys


def get_worksheet(spreadsheet: gspread.Spreadsheet, worksheet_name: str) -> gspread.Worksheet:
    try:
        return spreadsheet.worksheet(worksheet_name)
    except Exception as error:
        st.error(f"Unable to access worksheet '{worksheet_name}': {error}")
        st.stop()


@st.cache_data(ttl=120)
def fetch_worksheet_dataframe(
    sheet_url: str,
    service_account_json: str,
    worksheet_name: str,
    expected_headers: list[str],
):
    try:
        credentials = Credentials.from_service_account_info(
            json.loads(service_account_json), scopes=GOOGLE_SCOPES
        )
    except Exception as error:
        raise RuntimeError(f"Failed to build credentials: {error}")

    try:
        client = gspread.authorize(credentials)
        spreadsheet = client.open_by_url(sheet_url)
    except Exception as error:
        raise RuntimeError(f"Unable to open spreadsheet: {error}")

    worksheet_titles = [ws.title for ws in spreadsheet.worksheets()]
    if worksheet_name not in worksheet_titles:
        raise KeyError(
            f"Worksheet '{worksheet_name}' not found. Available worksheets: {worksheet_titles}"
        )

    try:
        worksheet = spreadsheet.worksheet(worksheet_name)
    except Exception as error:
        raise RuntimeError(f"Unable to access worksheet '{worksheet_name}': {error}")

    try:
        values = worksheet.get_all_values()
    except Exception as error:
        raise RuntimeError(f"Failed to read worksheet '{worksheet_name}': {error}")

    if not values:
        raise ValueError(
            f"Worksheet '{worksheet_name}' returned no data. Expected headers: {expected_headers}"
        )

    headers = values[0]
    if headers != expected_headers:
        raise ValueError(
            f"Header mismatch for '{worksheet_name}'. Expected {expected_headers} but found {headers}"
        )

    if len(values) <= 1:
        raise ValueError(f"Worksheet '{worksheet_name}' is empty beyond the header row.")

    return pd.DataFrame(values[1:], columns=headers)


def load_worksheet_dataframe(
    sheet_url: str,
    service_account_info: dict,
    worksheet_name: str,
    expected_headers: list[str],
):
    try:
        return fetch_worksheet_dataframe(
            sheet_url,
            json.dumps(service_account_info, sort_keys=True),
            worksheet_name,
            expected_headers,
        )
    except KeyError as error:
        st.error(str(error))
        st.stop()
    except ValueError as error:
        st.error(str(error))
        st.stop()
    except Exception as error:
        st.error(f"Unable to load '{worksheet_name}': {error}")
        st.stop()


def append_row_with_logging(
    worksheet: gspread.Worksheet,
    row_values: list,
    worksheet_label: str,
    destination=st,
):
    destination.info(f"Appending to {worksheet_label}: {row_values}")
    logger.info("Appending to %s: %s", worksheet_label, row_values)

    try:
        worksheet.append_row(row_values, value_input_option="USER_ENTERED")
        last_row = worksheet.get_all_values()[-1]
    except Exception as error:
        st.error(f"Failed to append to {worksheet_label}: {error}")
        st.stop()

    destination.success(f"Last row in {worksheet_label}: {last_row}")
    logger.info("Last row now: %s", last_row)
    return last_row


def render_boot_diagnostics():
    """Display secrets visibility before any fail-fast checks."""

    boot_panel = st.sidebar.container()
    boot_panel.subheader("Boot Diagnostics")
    visible_keys = list(st.secrets.keys())
    boot_panel.write(
        {
            "visible_keys": visible_keys,
            "has_sheet_url": "sheet_url" in st.secrets,
            "has_gcp_service_account": "gcp_service_account" in st.secrets,
        }
    )

    return visible_keys


def main():
    ensure_directories()

    render_boot_diagnostics()

    try:
        (
            spreadsheet,
            sheet_url,
            service_account_info,
            service_account_json,
            secret_keys,
        ) = require_google_sheet()
        tanker_dispensing_sheet = get_worksheet(spreadsheet, "Tanker Dispensing")
        tanker_receipts_sheet = get_worksheet(spreadsheet, "Tanker Receipts")
    except MissingSecretError as error:
        st.error(error)
        st.stop()
    except Exception as error:
        st.error(f"Failed to connect to Google Sheets: {error}")
        st.stop()

    database = load_data(DATABASE_FILE)

    st.sidebar.title("⛽ Fuel Command Center")
    st.sidebar.caption("Fast data entry and live analytics for tanker operations.")
    page = st.sidebar.radio(
        "Navigate", ["📝 Log Entry", "📊 Analytics Dashboard", "🛢️ Tanker Inventory"]
    )

    diagnostics_panel = st.sidebar.container()
    diagnostics_panel.markdown("---")
    diagnostics_panel.subheader("Diagnostics")
    diagnostics_panel.write({"st.secrets.keys()": secret_keys})
    diagnostics_panel.write(
        {
            "client_email": service_account_info.get("client_email"),
            "sheet_url": sheet_url,
            "spreadsheet_title": spreadsheet.title,
            "spreadsheet_id": spreadsheet.id,
        }
    )

    if diagnostics_panel.button("Test Sheets Write"):
        test_row = [
            datetime.utcnow().isoformat(),
            datetime.utcnow().date().isoformat(),
            "TEST",
            "TEST",
            0.01,
        ]
        append_row_with_logging(
            tanker_receipts_sheet,
            test_row,
            "Tanker Receipts",
            destination=diagnostics_panel,
        )

    if page == "📝 Log Entry":
        st.title("New Fuel Transaction")

        if database.empty:
            st.warning("Database is empty. Please check data/Database.csv")
            st.stop()

        operation_type = st.radio(
            "Select Operation:", ["Dispense to Fleet (OUT)", "Refill Tanker (IN)"], horizontal=True
        )

        if operation_type == "Dispense to Fleet (OUT)":
            st.info("Log fuel dispensing from a Tanker to a Vehicle/Equipment.")

            column_left, column_right = st.columns(2)

            with column_left:
                searchable_db = build_search_labels(database)
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
                tanker_options = get_tanker_options(database)
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
                        "Date": date,
                        "Fleet No": fleet_no,
                        "Asset ID": asset_row["Asset ID"],
                        "Category": category,
                        "Description": asset_row["Description"],
                        "Source Tanker": source_tanker,
                        "Fuel Out (L)": fuel_qty,
                        "Current Meter": current_meter,
                        "Meter Unit": meter_unit,
                    }

                    dispensing_row = [
                        datetime.utcnow().isoformat(),
                        date.isoformat(),
                        fleet_no,
                        asset_row["Asset ID"],
                        category,
                        asset_row["Description"],
                        source_tanker,
                        fuel_qty,
                        current_meter,
                        meter_unit,
                    ]
                    append_row_with_logging(
                        tanker_dispensing_sheet, dispensing_row, "Tanker Dispensing"
                    )

                    log_df = load_logs(VEHICLE_LOG_FILE, list(new_entry.keys()))
                    new_df = pd.DataFrame([new_entry])
                    log_df = pd.concat([log_df, new_df], ignore_index=True)
                    save_log(log_df, VEHICLE_LOG_FILE)

                    st.toast(f"Logged {fuel_qty}L for {fleet_no}!")
                    st.success(
                        f"✅ Transaction Saved. {fleet_no} took {fuel_qty}L from {source_tanker}."
                    )

        elif operation_type == "Refill Tanker (IN)":
            st.warning("Log fuel COMING IN to your Tankers from External Stations.")

            column_left, column_right = st.columns(2)
            with column_left:
                tanker_options = get_tanker_options(database)
                target_tanker = st.selectbox("Select Tanker Receiving Fuel:", options=tanker_options)

            with column_right:
                source_station = st.text_input("External Station Name (e.g., Shell Haima):")

            vol_in = st.number_input("Volume Received (Liters):", min_value=1)
            date_in = st.date_input("Date", datetime.today())

            if st.button("Log Refill"):
                entry = {
                    "Date": date_in,
                    "Tanker No": target_tanker,
                    "Source Station": source_station,
                    "Fuel In (L)": vol_in,
                }
                receipt_row = [
                    datetime.utcnow().isoformat(),
                    date_in.isoformat(),
                    target_tanker,
                    source_station,
                    vol_in,
                ]
                append_row_with_logging(
                    tanker_receipts_sheet, receipt_row, "Tanker Receipts"
                )
                log_df = load_logs(TANKER_LOG_FILE, list(entry.keys()))
                new_df = pd.DataFrame([entry])
                log_df = pd.concat([log_df, new_df], ignore_index=True)
                save_log(log_df, TANKER_LOG_FILE)

                st.success(f"✅ Added {vol_in}L to {target_tanker} Inventory.")

    elif page == "📊 Analytics Dashboard":
        st.title("Fuel Analytics")

        if st.button("🔄 Refresh data", type="secondary"):
            st.cache_data.clear()
            st.rerun()

        tanker_dispensing_df = load_worksheet_dataframe(
            sheet_url, service_account_info, "Tanker Dispensing", DISPENSING_HEADERS
        )
        tanker_receipts_df = load_worksheet_dataframe(
            sheet_url, service_account_info, "Tanker Receipts", RECEIPT_HEADERS
        )

        tanker_dispensing_df["Date"] = pd.to_datetime(
            tanker_dispensing_df["Date"], errors="coerce"
        )
        tanker_receipts_df["Date"] = pd.to_datetime(
            tanker_receipts_df["Date"], errors="coerce"
        )
        tanker_dispensing_df["Fuel Out (L)"] = pd.to_numeric(
            tanker_dispensing_df["Fuel Out (L)"], errors="coerce"
        )
        tanker_receipts_df["Fuel In (L)"] = pd.to_numeric(
            tanker_receipts_df["Fuel In (L)"], errors="coerce"
        )

        dispensing_dates = tanker_dispensing_df["Date"].dropna()
        receipt_dates = tanker_receipts_df["Date"].dropna()
        combined_dates = pd.concat([dispensing_dates, receipt_dates])

        if combined_dates.empty:
            st.error("No valid dates found in Google Sheets. Please verify data entries.")
            st.stop()

        min_date = combined_dates.min().date()
        max_date = combined_dates.max().date()

        filter_start, filter_end = st.date_input(
            "Filter date range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )

        filtered_dispensing = tanker_dispensing_df[
            tanker_dispensing_df["Date"].dt.date.between(filter_start, filter_end)
        ]

        if filtered_dispensing.empty:
            st.error("No dispensing data found for the selected date range.")
            st.stop()

        kpi1, kpi2, kpi3 = st.columns(3)
        total_fuel = filtered_dispensing["Fuel Out (L)"].sum()
        total_entries = len(filtered_dispensing)

        kpi1.metric("Total Fuel Consumed", f"{total_fuel:,.0f} L")
        kpi2.metric("Total Transactions", total_entries)
        kpi3.metric("Active Assets", filtered_dispensing["Fleet No"].nunique())

        st.markdown("---")

        chart_column_1, chart_column_2 = st.columns(2)

        with chart_column_1:
            st.subheader("Consumption by Category")
            chart_data = filtered_dispensing.groupby("Category")["Fuel Out (L)"].sum()
            st.bar_chart(chart_data)

        with chart_column_2:
            st.subheader("Top Consumers (Vehicles)")
            top_consumers = (
                filtered_dispensing.groupby("Fleet No")["Fuel Out (L)"]
                .sum()
                .sort_values(ascending=False)
                .head(5)
            )
            st.bar_chart(top_consumers)

        st.subheader("Recent Transactions")
        st.dataframe(
            filtered_dispensing.sort_values("Date", ascending=False).head(10),
            use_container_width=True,
        )

        with st.expander("Debug data summary", expanded=False):
            st.write(
                {
                    "row_counts": {
                        "Tanker Dispensing": len(tanker_dispensing_df),
                        "Tanker Receipts": len(tanker_receipts_df),
                    },
                    "date_bounds": {
                        "dispensing_min": dispensing_dates.min(),
                        "dispensing_max": dispensing_dates.max(),
                        "receipts_min": receipt_dates.min(),
                        "receipts_max": receipt_dates.max(),
                    },
                    "current_filter_range": {
                        "start": filter_start,
                        "end": filter_end,
                    },
                }
            )

    elif page == "🛢️ Tanker Inventory":
        st.title("Tanker Balances")
        st.write("Live tracking of fuel inside your 4 mobile tankers.")

        if st.button("🔄 Refresh data", type="secondary"):
            st.cache_data.clear()
            st.rerun()

        tanker_dispensing_df = load_worksheet_dataframe(
            sheet_url, service_account_info, "Tanker Dispensing", DISPENSING_HEADERS
        )
        tanker_receipts_df = load_worksheet_dataframe(
            sheet_url, service_account_info, "Tanker Receipts", RECEIPT_HEADERS
        )

        tanker_dispensing_df["Date"] = pd.to_datetime(
            tanker_dispensing_df["Date"], errors="coerce"
        )
        tanker_receipts_df["Date"] = pd.to_datetime(
            tanker_receipts_df["Date"], errors="coerce"
        )
        tanker_dispensing_df["Fuel Out (L)"] = pd.to_numeric(
            tanker_dispensing_df["Fuel Out (L)"], errors="coerce"
        )
        tanker_receipts_df["Fuel In (L)"] = pd.to_numeric(
            tanker_receipts_df["Fuel In (L)"], errors="coerce"
        )

        dispensing_dates = tanker_dispensing_df["Date"].dropna()
        receipt_dates = tanker_receipts_df["Date"].dropna()
        combined_dates = pd.concat([dispensing_dates, receipt_dates])

        if combined_dates.empty:
            st.error("No valid dates found in Google Sheets. Please verify data entries.")
            st.stop()

        min_date = combined_dates.min().date()
        max_date = combined_dates.max().date()

        filter_start, filter_end = st.date_input(
            "Filter date range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )

        filtered_dispensing = tanker_dispensing_df[
            tanker_dispensing_df["Date"].dt.date.between(filter_start, filter_end)
        ]
        filtered_receipts = tanker_receipts_df[
            tanker_receipts_df["Date"].dt.date.between(filter_start, filter_end)
        ]

        if filtered_dispensing.empty and filtered_receipts.empty:
            st.error("No tanker activity found for the selected date range.")
            st.stop()

        column_grid = st.columns(2)

        for index, tanker in enumerate(DEFAULT_TANKERS):
            total_in = filtered_receipts[
                filtered_receipts["Tanker No"] == tanker
            ]["Fuel In (L)"].sum()
            total_out = filtered_dispensing[
                filtered_dispensing["Source Tanker"] == tanker
            ]["Fuel Out (L)"].sum()

            current_balance = total_in - total_out

            with column_grid[index % 2]:
                st.container(border=True)
                st.subheader(f"🚛 {tanker}")
                st.metric("Current Level", f"{current_balance:,.0f} L")

                capacity = 30000
                percent = max(0.0, min(1.0, current_balance / capacity))
                st.progress(percent)
                st.caption(f"IN: {total_in} L | OUT: {total_out} L")

        with st.expander("Debug data summary", expanded=False):
            st.write(
                {
                    "row_counts": {
                        "Tanker Dispensing": len(tanker_dispensing_df),
                        "Tanker Receipts": len(tanker_receipts_df),
                    },
                    "date_bounds": {
                        "dispensing_min": dispensing_dates.min(),
                        "dispensing_max": dispensing_dates.max(),
                        "receipts_min": receipt_dates.min(),
                        "receipts_max": receipt_dates.max(),
                    },
                    "current_filter_range": {
                        "start": filter_start,
                        "end": filter_end,
                    },
                }
            )


if __name__ == "__main__":
    main()
