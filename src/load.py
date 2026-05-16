import requests
import pandas as pd
import logging
from datetime import datetime
import argparse
from pathlib import Path
import duckdb
import os

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

PROC_FILES = {
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
        "--db-path",
        type=str,
        required=True,
        help="Path to save results"
    )

    return parser.parse_args()

def ensure_dir_exists(path: str | Path) -> Path:
    logger.info("Checking if output path exists and is a directory")
    p = Path(path)

    if p.exists() and not p.is_dir():
        raise ValueError(f"Path exists but is not a directory: {p}")
    return True

def upsert_weather_hourly(con: duckdb.DuckDBPyConnection, df, table_name: str):
    # optional but recommended: keep the “latest” record per id
    logger.info(f"Inserting {table_name}")
    df = df.drop_duplicates(subset=["id"], keep="last")

    con.register("src_df", df)

    con.execute(f"""
        INSERT INTO {table_name} (
            time, temperature_2m, precipitation, windspeed_10m,
            latitude, longitude, time_request, id
        )
        SELECT
            time, temperature_2m, precipitation, windspeed_10m,
            latitude, longitude, time_request, id
        FROM src_df
        ON CONFLICT(id) DO UPDATE SET
            time = excluded.time,
            temperature_2m = excluded.temperature_2m,
            precipitation = excluded.precipitation,
            windspeed_10m = excluded.windspeed_10m,
            latitude = excluded.latitude,
            longitude = excluded.longitude,
            time_request = excluded.time_request,
            ingested_at = now();
    """)

def upsert_weather_daily(con: duckdb.DuckDBPyConnection, df, table_name: str):
    # optional but recommended: keep the “latest” record per id
    logger.info(f"Inserting {table_name}")
    df = df.drop_duplicates(subset=["id"], keep="last")

    con.register("src_df", df)

    con.execute(f"""
        INSERT INTO {table_name} (
            time, sunset, sunrise, daylight_duration,
            latitude, longitude, time_request, id
        )
        SELECT
            time, sunset, sunrise, daylight_duration,
            latitude, longitude, time_request, id
        FROM src_df
        ON CONFLICT(id) DO UPDATE SET
            time = excluded.time,
            sunset = excluded.sunset,
            sunrise = excluded.sunrise,
            daylight_duration = excluded.daylight_duration,
            latitude = excluded.latitude,
            longitude = excluded.longitude,
            time_request = excluded.time_request,
            ingested_at = now();
    """)

def load_dir_to_duckdb(proc_dir: str | Path, db_path: str | Path) -> None:
    proc_dir = Path(proc_dir)
    if not proc_dir.exists() or not proc_dir.is_dir():
        raise ValueError(f"Processed dir does not exist or is not a directory: {proc_dir}")

    logger.info(f"Loading processed dataframes from {proc_dir}")
    df_daily_forecast = pd.read_parquet(proc_dir / PROC_FILES["daily_forecast"])
    df_daily_past = pd.read_parquet(proc_dir / PROC_FILES["daily_past"])
    df_hourly_forecast = pd.read_parquet(proc_dir / PROC_FILES["hourly_forecast"])
    df_hourly_past = pd.read_parquet(proc_dir / PROC_FILES["hourly_past"])
    logger.info("Loading process completed.")

    logger.info("Connecting with database")
    conn = duckdb.connect(db_path)
    logger.info("Connection valid. Loading file into tables")

    upsert_weather_hourly(conn, df_hourly_past, "weather_hourly_past")
    upsert_weather_hourly(conn, df_hourly_forecast, "weather_hourly_forecast")
    upsert_weather_daily(conn, df_daily_past, "weather_daily_past")
    upsert_weather_daily(conn, df_daily_forecast, "weather_daily_forecast")
    logger.info("Loading dataframes into tables completed.")

    conn.close()

# python3 "src/load.py" --db-path "data/weather.duckdb" --input-path "rsc/data/proc_data"
if __name__ == "__main__":
    logger.info("Parsing arguments")
    args = parse_args()

    load_dir_to_duckdb(args.input_path, args.db_path)