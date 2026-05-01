import pandas as pd
import hashlib
from decimal import Decimal
from datetime import datetime
import logging
import argparse
from pathlib import Path

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


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
        type=bool, 
        required=True, 
        default=False, 
        help="Store in local output path")

    parser.add_argument(
        "--output-path",
        type=str,
        required=True,
        help="Path to save results"
    )

    return parser.parse_args()

def ensure_dir(path: str | Path) -> Path:
    p = Path(path)
    if p.exists() and not p.is_dir():
        raise ValueError(f"Path exists but is not a directory: {p}")
    p.mkdir(parents=True, exist_ok=True)
    return p

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


# python3 "src/transform.py" --input-path rsc/data/raw_data --local-store True --output-path rsc/data/proc_data
if __name__ == "__main__":
    args = parse_args()

    input_path = Path(args.input_path)
    if not input_path.exists() or not input_path.is_dir():
        raise ValueError(f"{input_path} does not exist or is not a directory. Please provide an existing input path")

    logger.info("Loading raw dataframes")
    df_hourly_past = pd.read_parquet(input_path / "df_hourly_past.parquet")
    df_hourly_forecast = pd.read_parquet(input_path / "df_hourly_forecast.parquet")
    df_daily_past = pd.read_parquet(input_path / "df_daily_past.parquet")
    df_daily_forecast = pd.read_parquet(input_path / "df_daily_forecast.parquet")

    logger.info("Cleaning data")
    df_hourly_past = clean_weather(df_hourly_past)
    df_hourly_forecast = clean_weather(df_hourly_forecast)
    df_daily_past = clean_weather(df_daily_past)
    df_daily_forecast = clean_weather(df_daily_forecast)

    if args.local_store:
        output_path = ensure_dir(args.output_path)

        logger.info(f"Storing processed dataset in {output_path}")
        df_hourly_past.to_parquet(output_path / "df_hourly_past.parquet")
        df_hourly_forecast.to_parquet(output_path / "df_hourly_forecast.parquet")
        df_daily_past.to_parquet(output_path / "df_daily_past.parquet")
        df_daily_forecast.to_parquet(output_path / "df_daily_forecast.parquet")

