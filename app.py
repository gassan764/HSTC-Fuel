import json
import logging
from datetime import datetime
from pathlib import Path

import gspread
import numpy as np
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
ASSETS_HEADERS = [
    "Fleet No",
    "Asset ID",
    "Category",
    "Description",
    "Plate Number",
    "Benchmark_KmL",
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

DEFAULT_DATE_RANGE_DAYS = 30
DATA_QUALITY_LIMITS = {
    "max_km_delta": 1500,
    "max_hour_delta": 100,
    "max_fuel_out": 800,
    "min_km_per_l": 1,
    "max_km_per_l": 25,
    "min_efficiency_ratio": 0.75,
    "max_efficiency_ratio": 1.25,
}


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


def _serialize_service_account(service_account_info) -> str:
    """Return a stable JSON string for caching client resources."""

    try:
        service_account_dict = dict(service_account_info)
    except TypeError:
        service_account_dict = service_account_info

    return json.dumps(service_account_dict, sort_keys=True)


@st.cache_resource
def get_gspread_client(service_account_json: str) -> gspread.Client:
    credentials = Credentials.from_service_account_info(
        json.loads(service_account_json), scopes=GOOGLE_SCOPES
    )
    return gspread.authorize(credentials)


@st.cache_resource
def open_spreadsheet(sheet_url: str, service_account_json: str) -> gspread.Spreadsheet:
    client = get_gspread_client(service_account_json)
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

    service_account_json = _serialize_service_account(service_account_info)
    spreadsheet = open_spreadsheet(sheet_url, service_account_json)

    return spreadsheet, sheet_url, service_account_json, secrets_keys


def get_worksheet(spreadsheet: gspread.Spreadsheet, worksheet_name: str) -> gspread.Worksheet:
    try:
        return spreadsheet.worksheet(worksheet_name)
    except Exception as error:
        st.error(f"Unable to access worksheet '{worksheet_name}': {error}")
        st.stop()


HEADERS_BY_WORKSHEET = {
    "Assets": ASSETS_HEADERS,
    "Tanker Dispensing": DISPENSING_HEADERS,
    "Tanker Receipts": RECEIPT_HEADERS,
}


@st.cache_data(ttl=120)
def load_worksheet_dataframe(
    sheet_url: str,
    worksheet_name: str,
    service_account_json: str,
):
    try:
        spreadsheet = open_spreadsheet(sheet_url, service_account_json)
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
        expected_headers = HEADERS_BY_WORKSHEET.get(worksheet_name, [])
        raise ValueError(
            f"Worksheet '{worksheet_name}' returned no data. Expected headers: {expected_headers}"
        )

    headers = [header.strip() for header in values[0]]
    required_headers = HEADERS_BY_WORKSHEET.get(worksheet_name, headers)
    missing_headers = [header for header in required_headers if header not in headers]

    if missing_headers:
        raise ValueError(
            "Header mismatch for '{worksheet}'. Missing required columns: {missing}. "
            "Found columns: {found}".format(
                worksheet=worksheet_name, missing=missing_headers, found=headers
            )
        )

    if len(values) <= 1:
        raise ValueError(f"Worksheet '{worksheet_name}' is empty beyond the header row.")

    return pd.DataFrame(values[1:], columns=headers)


def safe_load_worksheet_dataframe(
    sheet_url: str,
    worksheet_name: str,
    service_account_json: str,
):
    try:
        return load_worksheet_dataframe(sheet_url, worksheet_name, service_account_json)
    except KeyError as error:
        st.error(str(error))
        st.stop()
    except ValueError as error:
        st.error(str(error))
        st.stop()
    except Exception as error:
        st.error(f"Unable to load '{worksheet_name}': {error}")
        st.stop()


def normalize_headers(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe is None:
        return pd.DataFrame()

    normalized = dataframe.copy()
    normalized.columns = [str(column).strip() for column in normalized.columns]
    return normalized


def ensure_string_columns(dataframe: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    updated = dataframe.copy()
    for column in columns:
        if column in updated.columns:
            updated[column] = updated[column].astype(str).str.strip()
    return updated


def parse_event_datetime(dataframe: pd.DataFrame, timestamp_col="Timestamp", date_col="Date"):
    if dataframe.empty:
        dataframe["Event Datetime"] = pd.NaT
        return dataframe

    parsed = dataframe.copy()
    parsed[timestamp_col] = pd.to_datetime(parsed.get(timestamp_col), errors="coerce")
    parsed[date_col] = pd.to_datetime(parsed.get(date_col), errors="coerce")
    parsed["Event Datetime"] = parsed[timestamp_col].combine_first(parsed[date_col])
    return parsed


def merge_assets_with_dispensing(dispensing_df: pd.DataFrame, assets_df: pd.DataFrame) -> pd.DataFrame:
    dispensing_df = normalize_headers(dispensing_df)
    assets_df = normalize_headers(assets_df)

    dispensing_df = ensure_string_columns(dispensing_df, ["Fleet No", "Asset ID"])
    assets_df = ensure_string_columns(assets_df, ["Fleet No", "Asset ID"])

    merged = dispensing_df.merge(
        assets_df[
            [
                "Fleet No",
                "Asset ID",
                "Category",
                "Description",
                "Plate Number",
                "Benchmark_KmL",
            ]
        ],
        on="Fleet No",
        how="left",
        suffixes=("", "_asset"),
    )

    fallback_lookup = assets_df.set_index("Asset ID")

    for column in ["Asset ID", "Category", "Description", "Plate Number", "Benchmark_KmL"]:
        asset_column = f"{column}_asset"
        if asset_column in merged.columns:
            merged[column] = merged[asset_column].combine_first(merged[column])
            merged.drop(columns=[asset_column], inplace=True)

        if column in fallback_lookup.columns:
            merged[column] = merged[column].combine_first(
                merged["Asset ID"].map(fallback_lookup[column])
            )

    merged["Asset Key"] = merged["Fleet No"].fillna("").replace("", pd.NA)
    merged["Asset Key"] = merged["Asset Key"].combine_first(merged["Asset ID"])

    return merged


def build_consumption_metrics(merged_df: pd.DataFrame) -> pd.DataFrame:
    if merged_df.empty:
        return pd.DataFrame()

    analytics = merged_df.copy()
    analytics = analytics.sort_values(["Asset Key", "Event Datetime"])
    analytics["Previous Meter"] = analytics.groupby("Asset Key")["Current Meter"].shift(1)
    analytics["Meter Delta"] = analytics["Current Meter"] - analytics["Previous Meter"]

    analytics["Fuel Out (L)"] = pd.to_numeric(analytics["Fuel Out (L)"], errors="coerce")
    analytics["Meter Delta"] = pd.to_numeric(analytics["Meter Delta"], errors="coerce")
    analytics["Benchmark_KmL"] = pd.to_numeric(analytics.get("Benchmark_KmL"), errors="coerce")

    analytics["Meter Unit Normalized"] = analytics["Meter Unit"].str.lower().str.strip()

    km_mask = analytics["Meter Unit Normalized"] == "km"
    hour_mask = analytics["Meter Unit Normalized"].isin(["hour", "hours"])

    analytics.loc[km_mask, "Actual Km/L"] = analytics.loc[km_mask, "Meter Delta"] / analytics.loc[
        km_mask, "Fuel Out (L)"
    ]
    analytics.loc[km_mask, "Efficiency Ratio"] = analytics.loc[km_mask, "Actual Km/L"] / analytics.loc[
        km_mask, "Benchmark_KmL"
    ]

    analytics.loc[hour_mask, "Actual L/hour"] = analytics.loc[hour_mask, "Fuel Out (L)"] / analytics.loc[
        hour_mask, "Meter Delta"
    ]

    analytics.replace([np.inf, -np.inf], np.nan, inplace=True)

    return analytics


def build_data_quality_flags(analytics_df: pd.DataFrame, limits: dict) -> tuple[pd.DataFrame, dict]:
    if analytics_df.empty:
        return pd.DataFrame(), {}

    issues = []
    for _, row in analytics_df.iterrows():
        reasons = []
        unit = str(row.get("Meter Unit Normalized", "")).lower()
        delta = row.get("Meter Delta")
        fuel_out = row.get("Fuel Out (L)")
        actual_km_per_l = row.get("Actual Km/L")
        efficiency_ratio = row.get("Efficiency Ratio")

        if pd.isna(delta) or delta <= 0:
            reasons.append("Missing/invalid meter delta")

        if pd.isna(fuel_out) or fuel_out <= 0:
            reasons.append("Fuel Out (L) <= 0")

        if unit not in {"km", "hour", "hours"}:
            reasons.append("Meter Unit missing or not recognized")

        if unit == "km" and pd.notna(delta) and delta > limits["max_km_delta"]:
            reasons.append("Extreme km delta")

        if unit in {"hour", "hours"} and pd.notna(delta) and delta > limits["max_hour_delta"]:
            reasons.append("Extreme hour delta")

        if pd.notna(fuel_out) and fuel_out > limits["max_fuel_out"]:
            reasons.append("Fuel Out too large")

        if unit == "km" and pd.notna(actual_km_per_l):
            if actual_km_per_l < limits["min_km_per_l"] or actual_km_per_l > limits["max_km_per_l"]:
                reasons.append("Efficiency outlier (Km/L)")

        if pd.notna(efficiency_ratio):
            if (
                efficiency_ratio < limits["min_efficiency_ratio"]
                or efficiency_ratio > limits["max_efficiency_ratio"]
            ):
                reasons.append("Benchmark efficiency outlier")

        if reasons:
            issue_row = row.copy()
            issue_row["Issues"] = "; ".join(reasons)
            issues.append(issue_row)

    if not issues:
        return pd.DataFrame(), {}

    issues_df = pd.DataFrame(issues)
    counts = issues_df["Issues"].str.get_dummies(sep="; ").sum().to_dict()
    return issues_df, counts


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
        spreadsheet, sheet_url, service_account_json, secret_keys = require_google_sheet()
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
            "client_email": json.loads(service_account_json).get("client_email"),
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

        if st.button("Refresh data", type="secondary"):
            st.cache_data.clear()
            st.cache_resource.clear()
            st.rerun()

        assets_df = safe_load_worksheet_dataframe(sheet_url, "Assets", service_account_json)
        tanker_dispensing_df = safe_load_worksheet_dataframe(
            sheet_url, "Tanker Dispensing", service_account_json
        )
        tanker_receipts_df = safe_load_worksheet_dataframe(
            sheet_url, "Tanker Receipts", service_account_json
        )

        assets_df = normalize_headers(assets_df)
        tanker_dispensing_df = normalize_headers(tanker_dispensing_df)
        tanker_receipts_df = normalize_headers(tanker_receipts_df)

        tanker_dispensing_df["Fuel Out (L)"] = pd.to_numeric(
            tanker_dispensing_df["Fuel Out (L)"], errors="coerce"
        )
        tanker_receipts_df["Fuel In (L)"] = pd.to_numeric(
            tanker_receipts_df["Fuel In (L)"], errors="coerce"
        )
        tanker_dispensing_df["Current Meter"] = pd.to_numeric(
            tanker_dispensing_df["Current Meter"], errors="coerce"
        )

        tanker_dispensing_df = parse_event_datetime(tanker_dispensing_df)
        tanker_receipts_df = parse_event_datetime(tanker_receipts_df)

        event_dates = tanker_dispensing_df["Event Datetime"].dropna()
        if event_dates.empty:
            st.error("No valid Timestamp/Date data found in Tanker Dispensing worksheet.")
            st.stop()

        min_date_ts = event_dates.min()
        max_date_ts = event_dates.max()
        min_date = min_date_ts.date()
        max_date = max_date_ts.date()
        default_start = max_date_ts - pd.Timedelta(days=DEFAULT_DATE_RANGE_DAYS - 1)
        default_start = max(default_start.date(), min_date)

        merged_dispensing = merge_assets_with_dispensing(tanker_dispensing_df, assets_df)
        merged_dispensing = build_consumption_metrics(merged_dispensing)

        filter_container = st.container()
        with filter_container:
            st.subheader("Filters")
            col1, col2 = st.columns(2)
            with col1:
                filter_start, filter_end = st.date_input(
                    "Date range",
                    value=(default_start, max_date),
                    min_value=min_date,
                    max_value=max_date,
                )
                selected_categories = st.multiselect(
                    "Category",
                    options=sorted(
                        pd.Series(merged_dispensing.get("Category", pd.Series([]))).dropna().unique()
                    ),
                )
                selected_fleet = st.multiselect(
                    "Fleet No",
                    options=sorted(merged_dispensing.get("Fleet No", pd.Series([])).dropna().unique()),
                )
                selected_asset_ids = st.multiselect(
                    "Asset ID",
                    options=sorted(merged_dispensing.get("Asset ID", pd.Series([])).dropna().unique()),
                )
            with col2:
                selected_tankers = st.multiselect(
                    "Source Tanker",
                    options=sorted(
                        merged_dispensing.get("Source Tanker", pd.Series([])).dropna().unique()
                    ),
                )
                meter_unit_filter = st.selectbox(
                    "Meter Unit",
                    options=["All", "km", "hour"],
                    help="Choose whether to show all meter units or filter by kilometers/hours.",
                )
                only_with_benchmark = st.checkbox(
                    "Only show assets with Benchmark_KmL",
                    value=False,
                )
                st.caption("Refresh to reload Google Sheets data")

        filtered_df = merged_dispensing[
            merged_dispensing["Event Datetime"].dt.date.between(filter_start, filter_end)
        ]

        if selected_categories:
            filtered_df = filtered_df[filtered_df["Category"].isin(selected_categories)]

        if selected_fleet:
            filtered_df = filtered_df[filtered_df["Fleet No"].isin(selected_fleet)]

        if selected_asset_ids:
            filtered_df = filtered_df[filtered_df["Asset ID"].isin(selected_asset_ids)]

        if selected_tankers:
            filtered_df = filtered_df[filtered_df["Source Tanker"].isin(selected_tankers)]

        if meter_unit_filter != "All":
            if meter_unit_filter == "km":
                filtered_df = filtered_df[filtered_df["Meter Unit Normalized"] == "km"]
            else:
                filtered_df = filtered_df[
                    filtered_df["Meter Unit Normalized"].isin(["hour", "hours"])
                ]

        if only_with_benchmark:
            filtered_df = filtered_df[
                filtered_df["Benchmark_KmL"].notna()
                & (pd.to_numeric(filtered_df["Benchmark_KmL"], errors="coerce") > 0)
            ]

        if filtered_df.empty:
            st.error("No dispensing data found for the selected filters.")
            st.stop()

        kpi1, kpi2, kpi3 = st.columns(3)
        kpi1.metric("Total Fuel Consumed", f"{filtered_df['Fuel Out (L)'].sum():,.0f} L")
        kpi2.metric("Total Transactions", len(filtered_df))
        kpi3.metric("Active Assets", filtered_df["Asset Key"].nunique())

        limits_container = st.expander("Data quality limits", expanded=False)
        with limits_container:
            col_limit_a, col_limit_b, col_limit_c = st.columns(3)
            with col_limit_a:
                max_km_delta = st.number_input(
                    "Max km per fueling", value=DATA_QUALITY_LIMITS["max_km_delta"], min_value=1
                )
                min_km_per_l = st.number_input(
                    "Min Km/L", value=float(DATA_QUALITY_LIMITS["min_km_per_l"]), min_value=0.0
                )
                max_km_per_l = st.number_input(
                    "Max Km/L", value=float(DATA_QUALITY_LIMITS["max_km_per_l"]), min_value=0.0
                )
            with col_limit_b:
                max_hour_delta = st.number_input(
                    "Max hours per fueling",
                    value=DATA_QUALITY_LIMITS["max_hour_delta"],
                    min_value=1,
                )
                max_fuel_out = st.number_input(
                    "Max Fuel Out (L)",
                    value=DATA_QUALITY_LIMITS["max_fuel_out"],
                    min_value=1,
                )
            with col_limit_c:
                min_eff_ratio = st.number_input(
                    "Min Efficiency Ratio",
                    value=float(DATA_QUALITY_LIMITS["min_efficiency_ratio"]),
                    min_value=0.0,
                    step=0.05,
                )
                max_eff_ratio = st.number_input(
                    "Max Efficiency Ratio",
                    value=float(DATA_QUALITY_LIMITS["max_efficiency_ratio"]),
                    min_value=0.0,
                    step=0.05,
                )

        limits = {
            "max_km_delta": max_km_delta,
            "max_hour_delta": max_hour_delta,
            "max_fuel_out": max_fuel_out,
            "min_km_per_l": min_km_per_l,
            "max_km_per_l": max_km_per_l,
            "min_efficiency_ratio": min_eff_ratio,
            "max_efficiency_ratio": max_eff_ratio,
        }

        issues_df, issue_counts = build_data_quality_flags(filtered_df, limits)

        st.markdown("---")
        st.subheader("Data Quality")
        col_issue_a, col_issue_b = st.columns([1, 2])
        with col_issue_a:
            st.metric("Rows with issues", len(issues_df))
            if issue_counts:
                st.write(issue_counts)
        with col_issue_b:
            if issues_df.empty:
                st.success("No data quality issues detected for the current filters.")
            else:
                st.dataframe(issues_df[[
                    "Event Datetime",
                    "Fleet No",
                    "Asset ID",
                    "Source Tanker",
                    "Fuel Out (L)",
                    "Current Meter",
                    "Previous Meter",
                    "Meter Delta",
                    "Actual Km/L",
                    "Actual L/hour",
                    "Efficiency Ratio",
                    "Issues",
                ]], use_container_width=True)

        st.markdown("---")
        st.subheader("Asset Performance")
        perf_col_a, perf_col_b = st.columns(2)

        km_rows = filtered_df[
            (filtered_df["Meter Unit Normalized"] == "km")
            & filtered_df["Actual Km/L"].notna()
            & filtered_df["Benchmark_KmL"].notna()
            & filtered_df["Efficiency Ratio"].notna()
        ]
        hour_rows = filtered_df[
            filtered_df["Meter Unit Normalized"].isin(["hour", "hours"])
            & filtered_df["Actual L/hour"].notna()
        ]

        with perf_col_a:
            st.caption("Top/Bottom by efficiency ratio (Km/L vs Benchmark)")
            if km_rows.empty:
                st.info("No benchmark data available for selected filters.")
            else:
                ratio_summary = (
                    km_rows.groupby("Asset Key")[["Fuel Out (L)", "Efficiency Ratio"]]
                    .agg({"Fuel Out (L)": "sum", "Efficiency Ratio": "mean"})
                    .reset_index()
                )
                top10 = ratio_summary.sort_values("Efficiency Ratio", ascending=False).head(10)
                bottom10 = ratio_summary.sort_values("Efficiency Ratio", ascending=True).head(10)
                st.write("Top 10 Assets by Efficiency Ratio")
                st.dataframe(top10, use_container_width=True)
                st.write("Bottom 10 Assets by Efficiency Ratio")
                st.dataframe(bottom10, use_container_width=True)

        with perf_col_b:
            st.caption("Hour-meter efficiency and fuel volume leaders")
            if hour_rows.empty:
                st.info("No hour-based efficiency data available for selected filters.")
            else:
                worst_hours = (
                    hour_rows.groupby("Asset Key")["Actual L/hour"].mean().sort_values(ascending=False)
                )
                st.write("Worst 10 Assets by Actual L/hour")
                st.dataframe(worst_hours.head(10), use_container_width=True)

        consumer_col1, consumer_col2 = st.columns(2)
        with consumer_col1:
            st.caption("Highest fuel consumers")
            top_consumers = (
                filtered_df.groupby("Asset Key")["Fuel Out (L)"].sum().sort_values(ascending=False)
            )
            st.bar_chart(top_consumers.head(10))
        with consumer_col2:
            st.caption("Most frequent fueling assets")
            freq_assets = filtered_df.groupby("Asset Key").size().sort_values(ascending=False)
            st.bar_chart(freq_assets.head(10))

        unique_assets = filtered_df["Asset Key"].dropna().unique()
        if len(unique_assets) == 1:
            st.markdown("---")
            st.subheader("Asset Trend")
            asset_df = filtered_df.sort_values("Event Datetime")
            asset_df["Rolling Efficiency"] = (
                asset_df["Actual Km/L"].fillna(asset_df["Actual L/hour"])
            ).rolling(5, min_periods=1).mean()

            trend_cols = st.columns(3)
            with trend_cols[0]:
                st.line_chart(asset_df.set_index("Event Datetime")["Fuel Out (L)"])
            with trend_cols[1]:
                st.line_chart(asset_df.set_index("Event Datetime")["Meter Delta"])
            with trend_cols[2]:
                st.line_chart(asset_df.set_index("Event Datetime")[["Actual Km/L", "Actual L/hour", "Rolling Efficiency"]])
        else:
            st.info("Select exactly one Fleet No or Asset ID to view detailed trend charts.")

        st.markdown("---")
        st.subheader("Tanker Performance (dispensing)")
        tanker_group = filtered_df.groupby("Source Tanker")
        tanker_totals = tanker_group["Fuel Out (L)"].sum().sort_values(ascending=False)
        tanker_avg = tanker_group["Fuel Out (L)"].mean()
        st.bar_chart(tanker_totals)
        st.dataframe(
            pd.DataFrame(
                {
                    "Total Fuel Out": tanker_totals,
                    "Average per dispense": tanker_avg,
                }
            ),
            use_container_width=True,
        )

        with st.expander("Debug data summary", expanded=False):
            st.write(
                {
                    "row_counts": {
                        "Assets": len(assets_df),
                        "Tanker Dispensing": len(tanker_dispensing_df),
                        "Tanker Receipts": len(tanker_receipts_df),
                        "Merged": len(merged_dispensing),
                        "Filtered": len(filtered_df),
                    },
                    "date_bounds": {
                        "dispensing_min": event_dates.min(),
                        "dispensing_max": event_dates.max(),
                    },
                    "metric_counts": {
                        "km_rows": len(km_rows),
                        "hour_rows": len(hour_rows),
                        "quality_issues": len(issues_df),
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
            st.cache_resource.clear()
            st.rerun()

        tanker_dispensing_df = safe_load_worksheet_dataframe(
            sheet_url, "Tanker Dispensing", service_account_json
        )
        tanker_receipts_df = safe_load_worksheet_dataframe(
            sheet_url, "Tanker Receipts", service_account_json
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
