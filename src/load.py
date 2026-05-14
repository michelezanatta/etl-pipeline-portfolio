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



# python3 "src/load.py" --db-path "data/weather.duckdb" --input-path "rsc/data/proc_data"
if __name__ == "__main__":
    logger.info("Parsing arguments")
    args = parse_args()

    db_path = args.db_path
    input_path = args.input_path

    if not ensure_dir_exists(input_path):
        raise ValueError("Directory not existing.")

    logger.info(f"Loading processed dataframes from {input_path}")
    df_daily_forecast = pd.read_parquet(os.path.join(input_path, "df_daily_forecast.parquet"))
    df_daily_past = pd.read_parquet(os.path.join(input_path, "df_daily_past.parquet"))
    df_hourly_forecast = pd.read_parquet(os.path.join(input_path, "df_hourly_forecast.parquet"))
    df_hourly_past = pd.read_parquet(os.path.join(input_path, "df_hourly_past.parquet"))

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




