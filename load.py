import os
import json
import pandas as pd

RAW_FOLDER = r"C:\ETL_Logistics\raw"

def load_json_file(path):
    """Load a JSON file safely."""
    try:
        with open(path, "r") as f:
            data = json.load(f)
        return data
    except Exception as e:
        print(f"❌ Could not load {path}: {e}")
        return None

def extract_records(payload):
    """Extract rows from a JSON payload."""
    if not payload or "results" not in payload:
        return pd.DataFrame()

    rows = []
    for r in payload["results"]:
        rows.append({
            "location": r.get("location"),
            "pollutant": r.get("pollutant"),
            "value": r.get("value"),
            "timestamp": r.get("timestamp")
        })

    return pd.DataFrame(rows)

def load_all():
    """Load and merge all JSON files from raw folder."""
    files = [f for f in os.listdir(RAW_FOLDER) if f.endswith(".json")]

    print(f"📁 Found {len(files)} raw files.")

    all_dfs = []

    for file in files:
        payload = load_json_file(os.path.join(RAW_FOLDER, file))
        df = extract_records(payload)

        if not df.empty:
            print(f"📄 Loaded {len(df)} rows from {file}")
            all_dfs.append(df)
        else:
            print(f"⚠️ No valid rows in {file}")

    # If no data at all
    if not all_dfs:
        print("⚠️ No data found in any file.")
        return pd.DataFrame()

    combined = pd.concat(all_dfs, ignore_index=True)
    print(f"📊 Loaded {len(combined)} rows before cleaning.")

    # CLEAN DATA
    combined = combined.dropna(subset=["pollutant", "value"])
    print(f"🧹 {len(combined)} rows remain after removing empty pollutant rows.")

    return combined
