import pandas as pd
import hashlib
from decimal import Decimal
from datetime import datetime
import logging


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


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

    df = df.apply(generate_id, axis = 1)
    return df

if __name__ == "__main__":
    from extract import fetch_weather
    logger.info("Extracting weather data")
    df_hourly_past, df_hourly_forecast, df_daily_past, df_daily_forecast = fetch_weather()
    logger.info("Cleaning data")
    df_hourly_past = clean_weather(df_hourly_past)
    df_hourly_forecast = clean_weather(df_hourly_forecast)
    df_daily_past = clean_weather(df_daily_past)
    df_daily_forecast = clean_weather(df_daily_forecast)
    

    

