from extract import fetch_weather
from transform import clean_weather
from load import load_to_duckdb

def run_pipeline(latitude=41.90, longitude=12.49, days=7):
    print("Step 1/3 — Extracting data from Open-Meteo...")
    df = fetch_weather(latitude, longitude, days)
    print(f"  Fetched {len(df)} rows")
    print("Step 2/3 — Transforming data...")
    df = clean_weather(df)
    print(f"  Clean rows: {len(df)}")
    print("Step 3/3 — Loading into DuckDB...")
    load_to_duckdb(df)
    print("Pipeline complete!")
    
if __name__ == "__main__":
    run_pipeline() 