import pandas as pd
import hashlib
from decimal import Decimal
from datetime import datetime
import logging
import argparse
from pathlib import Path
from utils.paths import ensure_dir

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

RAW_FILES = {
    "hourly_past": "df_hourly_past.parquet",
    "hourly_forecast": "df_hourly_forecast.parquet",
    "daily_past": "df_daily_past.parquet",
    "daily_forecast": "df_daily_forecast.parquet",
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="ETL pipeline for weather data"
    )

    parser.add_argument(
        "--input-path",
        type=str,
        required=True,
        help="Path to save results"
    )

    parser.add_argument(
        "--local-store", 
        action="store_true", 
        help="Write Parquet files locally")

    parser.add_argument(
        "--output-path",
        type=str,
        required=True,
        help="Path to save results"
    )

    return parser.parse_args()

def generate_id(row):
    timestamp = row["time"]
    latitude = row["latitude"]
    longitude = row["longitude"]

    # Normalize inputs
    ts_str = timestamp.isoformat() if isinstance(timestamp, datetime) else str(timestamp)
    latitude_str = format(Decimal(str(latitude)), 'f')
    longitude_str = format(Decimal(str(longitude)), 'f')

    # Combine
    raw = f"{ts_str}|{latitude_str}|{longitude_str}"

    # Hash
    return hashlib.sha256(raw.encode()).hexdigest()

def clean_weather(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna()
    df = df.drop_duplicates(subset=["time", "latitude", "longitude"])
    df["ingested_at"] = pd.Timestamp.now()

    df["id"] = df.apply(generate_id, axis=1)
    return df

def transform_dir(raw_dir: str | Path, proc_dir: str | Path) -> dict[str, Path]:
    raw_dir = Path(raw_dir)
    if not raw_dir.exists() or not raw_dir.is_dir():
        raise ValueError(f"Raw dir does not exist or is not a directory: {raw_dir}")

    proc_dir = ensure_dir(proc_dir)

    logger.info("Loading raw dataframes")
    df_hourly_past = pd.read_parquet(raw_dir / RAW_FILES["hourly_past"])
    df_hourly_forecast = pd.read_parquet(raw_dir / RAW_FILES["hourly_forecast"])
    df_daily_past = pd.read_parquet(raw_dir / RAW_FILES["daily_past"])
    df_daily_forecast = pd.read_parquet(raw_dir / RAW_FILES["daily_forecast"])
    logger.info("Cleaning data")
    df_hourly_past = clean_weather(df_hourly_past)
    df_hourly_forecast = clean_weather(df_hourly_forecast)
    df_daily_past = clean_weather(df_daily_past)
    df_daily_forecast = clean_weather(df_daily_forecast)

    outputs = {
        "hourly_past": proc_dir / RAW_FILES["hourly_past"],
        "hourly_forecast": proc_dir / RAW_FILES["hourly_forecast"],
        "daily_past": proc_dir / RAW_FILES["daily_past"],
        "daily_forecast": proc_dir / RAW_FILES["daily_forecast"],
    }
    logger.info(f"Storing processed dataset in {proc_dir}")
    df_hourly_past.to_parquet(outputs["hourly_past"])
    df_hourly_forecast.to_parquet(outputs["hourly_forecast"])
    df_daily_past.to_parquet(outputs["daily_past"])
    df_daily_forecast.to_parquet(outputs["daily_forecast"])

    return outputs

# python3 "src/transform.py" --input-path rsc/data/raw_data --local-store --output-path rsc/data/proc_data
if __name__ == "__main__":
    args = parse_args()
    if args.local_store:
        transform_dir(args.input_path, args.output_path)