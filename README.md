# HSTC-Fuel

This repository captures a lightweight prototype for fuel tracking across a mixed fleet and four mobile tankers (BPS-95, HSC-116, BPS-13, HSC-101). The goal is to keep Google Sheets as the system of record and feed a dashboard/Streamlit front-end for faster data entry and anomaly checks.

## Current data snapshot

The repository includes a sample `data/Database.csv` that mirrors 347 assets across four categories (Vehicles, Buses, Machines/Equipment, and Tankers). Use it to seed your Google Sheet’s **Assets** tab if you need a starting point. The tankers present are:

- **HSC-101** — Plate: HA 948
- **HSC-116** — Plate: DA 4247
- **BPS-13** — Plate: MR 4557
- **BPS-95** — Plate: Unknown

Columns available today: `Fleet No`, `Asset ID`, `Category`, `Description`, `Plate Number`, `Benchmark_KmL` (used only for vehicle consumption checks).

## Recommended Google Sheets structure

Keep everything in a single Google Sheet with four tabs. All entries are append-only to simplify syncing with a dashboard (via Streamlit or similar) using the Sheets API.

1. **Assets (master data)**
   - Columns: `Fleet No` (text, unique), `Asset ID`, `Category` (Data validation list: Vehicle, Bus, Equipment, Machine, Tanker), `Description`, `Plate Number`, `Benchmark_KmL` (nullable except Vehicles).
   - Purpose: Lookup table for autofill and validation.

2. **Tanker Receipts (fuel coming IN from stations)**
   - Columns: `Timestamp`, `Date`, `Tanker No` (validated against Tanker entries in Assets), `Source Station` (text), `Fuel In (L)` (number), `Receipt/Reference` (optional).
   - Checks: Require `Fuel In (L) > 0`; protect formulas if added later.

3. **Tanker Dispensing (fuel going OUT to fleet)**
   - Columns: `Timestamp`, `Date`, `Fleet No` (validated against Assets), `Category` (filled via VLOOKUP), `Source Tanker` (validated Tanker list), `Fuel Out (L)`, `Current Meter`, `Meter Unit` (Km/Hours derived from category), `Operator` (optional), `Remarks` (optional).
   - Checks: `Fuel Out (L) > 0`, meter must be non-decreasing per asset (can flag via conditional formatting), benchmark variance for vehicles (see below).

4. **Derived/Balance sheet (read-only)**
   - Columns per tanker: `Total In`, `Total Out`, `Balance = In - Out`, `Capacity`, `% Full`. Populate via `SUMIF` from Receipts/Dispensing sheets. Include a pivot summarizing fuel by Category and Top Assets.

## Validation and automation tips

- **Autocomplete search:** In Streamlit, `st.selectbox` already supports type-to-search; to optimize for partial codes (e.g., typing `116`), expose a dedicated search field that filters the select list before selection. In Google Sheets, enable data validation with dropdown+search (new dropdown chips) for the `Fleet No` and `Source Tanker` cells.
- **Fast entry on Sheets:** Freeze header rows; protect formula columns; use input tabs with only unlocked cells. Add AppScript or a simple on-edit trigger to stamp `Timestamp` automatically when a new row is added.
- **Benchmark checks (vehicles only):** Add calculated columns on the Dispensing sheet:
  - `Last Meter` via `=IFERROR(VLOOKUP(...), "")`
  - `Distance` = `Current Meter - Last Meter`
  - `Expected Fuel` = `Distance / Benchmark_KmL`
  - `Variance (L)` = `Fuel Out (L) - Expected Fuel`
  - Use conditional formatting to flag variance beyond ±10–15%.
- **Balance validation:** On the Derived sheet, compute `Balance` per tanker; compare against physical dip-stick readings when available. Highlight negative balances.
- **Data quality:** Enforce uniqueness on `Fleet No` in the Assets sheet. Keep Benchmark_KmL blank for non-vehicles to avoid false flags.

## Dashboard layout suggestion

- **Navigation:** Tabs for “Log Entry”, “Analytics”, and “Tanker Balances”.
- **Log Entry:** Two modes—Refill Tanker (IN) and Dispense to Fleet (OUT). Autofill Category/Description/Plate after selecting Fleet No. Disable submission if required fields are missing.
- **Analytics:** KPIs (Total fuel dispensed, #Transactions, Active assets), bar charts (by Category, Top 5 assets), and a recent transactions table sourced directly from the Dispensing sheet.
- **Tanker Balances:** Show current liters per tanker with capacity progress bars using `Total In - Total Out`. Optionally include last refill date and last dispense date.

## Syncing the dashboard with Google Sheets

- Use a service account with Sheets API access and store credentials as environment variables or a secrets file (never commit keys).
- Replace the CSV loaders in `app.py` with `gspread`/`pandas` readers pointing to the four tabs above. Cache reads with `st.cache_data` and write via append-only API calls.
- If hosting on Streamlit Community Cloud or another PaaS, keep Google credentials in the platform’s secret manager.

These changes make Google Sheets the single source of truth while still enabling fast dashboard entry with searchable dropdowns and automated validation.

## Running the Streamlit demo locally

1. Install dependencies: `pip install -r requirements.txt`.
2. Configure Streamlit secrets (e.g., `.streamlit/secrets.toml`) with:
   - `gcp_service_account` — full JSON object for the service account.
   - `sheet_url` — URL of the Google Sheet containing tabs **Assets**, **Tanker Dispensing**, and **Tanker Receipts** (created automatically if missing).
3. Populate the **Assets** tab in your Google Sheet (you can import `data/Database.csv` as a starter list).
4. Launch: `streamlit run app.py`.
5. Use the **📝 Log Entry** tab to record dispensing (OUT) or tanker refills (IN).
6. Review **📊 Analytics Dashboard** and **🛢️ Tanker Inventory** to validate totals.

All reads and writes occur directly in Google Sheets—no CSV logs are persisted locally.

## Switching from CSV to Google Sheets

- Replace the `load_data`, `load_logs`, and `save_log` helpers in `app.py` with `gspread`/Sheets API calls to append rows to the four tabs described above. Keep the same column names so the UI continues working.
- Store Google service account credentials in Streamlit secrets or environment variables.
- If you need to point at a different sheet/tab during testing, export the sheet as CSV and drop it into the repo with the same filename, or wrap `DATABASE_FILE` with your connector function.
