import pandas as pd
import sqlite3
import os

# --- Configuration ---
GOOGLE_SHEET_CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vT-UtS5s5vPLOFUv0DKdhhmx6nFYne96d4uothRkTrP3nxvhDFDpHM9zCRIYmyXwBo1uf4QE-oX6FkN/pub?output=csv"
DB_FILE_NAME = "hotel_data.db"
TABLE_NAME = "daily_hourly_metrics"

# --- Main Sync Function ---
def sync_google_sheet_to_sqlite():
    print(f"Starting data sync to {DB_FILE_NAME}...")

    # 1. Read data from Google Sheet CSV URL
    try:
        df = pd.read_csv(GOOGLE_SHEET_CSV_URL)
        print("Successfully read data from Google Sheet.")
        print(f"Initial columns: {df.columns.tolist()}")
        print(f"Initial row count: {len(df)}")

    except Exception as e:
        print(f"Error reading Google Sheet: {e}")
        print("Please ensure the URL is correct and the sheet is published to web as CSV.")
        return

    # 2. Data Cleaning and Type Conversion
    # Combine 'Date' and 'Time' into a single datetime column for easier indexing/querying
    try:
        df['DateTime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])
    except KeyError as e:
        print(f"Error: Missing expected column '{e}' in Google Sheet. Please check column labels.")
        return
    except Exception as e:
        print(f"Error combining Date and Time columns: {e}")
        return

    # Only convert truly numeric columns
    numeric_cols = ['Rooms Sold', 'Rooms Available', 'Arrivals', 'OOO Rooms']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            print(f"Converted '{col}' to numeric and filled NaNs.")
        else:
            print(f"Warning: Column '{col}' not found in Google Sheet. Skipping conversion.")


    for col in numeric_cols:
        if col in df.columns:
            # Use pd.to_numeric with errors='coerce' to turn non-numeric values (like 'sold out') into NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')
            # Fill NaN values (e.g., from 'sold out') with 0, or any other default you prefer
            df[col] = df[col].fillna(0)
            print(f"Converted '{col}' to numeric and filled NaNs.")
        else:
            print(f"Warning: Column '{col}' not found in Google Sheet. Skipping conversion.")

    # Select and reorder columns for clarity in the DB table
    # 'DateTime' will be primary timestamp, remove original 'Date' and 'Time'
    final_columns = [
        'DateTime',
        'Rooms Sold',
        'Rooms Available',
        'Arrivals',
        'OOO Rooms',
        'King Rate',
        'QQ Rate'
    ]
    # Filter df to only include the final_columns that actually exist
    df_cleaned = df[[col for col in final_columns if col in df.columns]].copy()

    # 3. Connect to SQLite database and write data
    try:
        conn = sqlite3.connect(DB_FILE_NAME)
        # Use 'replace' to overwrite the table completely each sync
        df_cleaned.to_sql(TABLE_NAME, conn, if_exists='replace', index=False)
        conn.close()
        print(f"Successfully synced data to {DB_FILE_NAME}, table '{TABLE_NAME}'.")
        print(f"Final row count in DB: {len(df_cleaned)}")
        print("DB path:", os.path.abspath(DB_FILE_NAME))

    except Exception as e:
        print(f"Error writing to SQLite database: {e}")
        print("Please ensure SQLite is correctly installed and permissions are set.")

# --- Run the Sync ---
if __name__ == "__main__":
    sync_google_sheet_to_sqlite()