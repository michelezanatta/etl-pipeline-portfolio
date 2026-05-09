import duckdb
import pandas as pd
import logging
from datetime import datetime
import argparse
from pathlib import Path

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


def parse_args():
    parser = argparse.ArgumentParser(
        description="ETL pipeline for weather data"
    )

    parser.add_argument(
        "--db-path",
        type=str,
        required=True,
        help="Path to save results"
    )

    return parser.parse_args()

def ensure_dir(path: str | Path) -> Path:
    logger.info("Checking if output path exists and is a directory")
    p = Path(path)

    if p.exists() and not p.is_dir():
        raise ValueError(f"Path exists but is not a directory: {p}")

    p.mkdir(parents=True, exist_ok=True)
    return p


# python3 "src/create_db.py" --db-path "data/weather.duckdb"
if __name__ == "__main__":
    logger.info("Parsing arguments")
    args = parse_args()


    db_path = args.db_path
    logger.info(f"DB path {db_path}")


    logger.info("Connecting with database")
    conn = duckdb.connect(db_path)
    logger.info("Connection valid")

    query_create_table_hourly_past = """
                                        CREATE TABLE IF NOT EXISTS weather_hourly_past (
                                            time TIMESTAMP,
                                            temperature_2m DOUBLE,
                                            precipitation DOUBLE,
                                            windspeed_10m DOUBLE,
                                            latitude DOUBLE,
                                            longitude DOUBLE,
                                            time_request TIMESTAMP,
                                            ingested_at TIMESTAMP DEFAULT now(),
                                            id STRING
                                            );
                                        """

    query_create_table_hourly_forecast = """
                                        CREATE TABLE IF NOT EXISTS weather_hourly_forecast (
                                            time TIMESTAMP,
                                            temperature_2m DOUBLE,
                                            precipitation DOUBLE,
                                            windspeed_10m DOUBLE,
                                            latitude DOUBLE,
                                            longitude DOUBLE,
                                            time_request TIMESTAMP,
                                            ingested_at TIMESTAMP DEFAULT now(),
                                            id STRING
                                            );
                                        """


    query_create_table_daily_past = """ 

                                CREATE TABLE IF NOT EXISTS weather_daily_past (
                                    time TIMESTAMP,
                                    sunset TIMESTAMP,
                                    sunrise TIMESTAMP,
                                    daylight_duration DOUBLE,
                                    latitude DOUBLE,
                                    longitude DOUBLE,
                                    time_request TIMESTAMP,
                                    ingested_at TIMESTAMP DEFAULT now(),
                                    id STRING
                                    );

    """

    query_create_table_daily_forecast = """ 

    CREATE TABLE IF NOT EXISTS weather_daily_forecast (
        time TIMESTAMP,
        sunset TIMESTAMP,
        sunrise TIMESTAMP,
        daylight_duration DOUBLE,
        latitude DOUBLE,
        longitude DOUBLE,
        time_request TIMESTAMP,
        ingested_at TIMESTAMP DEFAULT now(),
        id STRING
        );

    """

    logger.info(f"Running query {query_create_table_hourly_past}")
    conn.execute(query=query_create_table_hourly_past)
    logger.info(f"Query completed.")
    logger.info(f"Running query {query_create_table_hourly_forecast}")
    conn.execute(query=query_create_table_hourly_forecast)
    logger.info(f"Query completed.")
    logger.info(f"Running query {query_create_table_daily_past}")
    conn.execute(query=query_create_table_daily_past)
    logger.info(f"Query completed.")
    logger.info(f"Running query {query_create_table_daily_forecast}")
    conn.execute(query_create_table_daily_forecast)
    logger.info(f"Query completed.")

    query_unique_id_hourly_past = """CREATE UNIQUE INDEX IF NOT EXISTS ux_weather_hourly_past_id
    ON weather_hourly_past(id);"""

    query_unique_id_hourly_forecast = """CREATE UNIQUE INDEX IF NOT EXISTS ux_weather_hourly_forecast_id
    ON weather_hourly_forecast(id);"""

    query_unique_id_daily_past = """CREATE UNIQUE INDEX IF NOT EXISTS ux_weather_daily_past_id
    ON weather_daily_past(id);"""

    query_unique_id_daily_forecast = """CREATE UNIQUE INDEX IF NOT EXISTS ux_weather_daily_forecast_id
    ON weather_daily_forecast(id);"""

    logger.info("Creating unique index")
    logger.info(f"Running query {query_unique_id_hourly_past}")
    conn.execute(query=query_unique_id_hourly_past)
    logger.info(f"Query completed.")
    logger.info(f"Running query {query_unique_id_hourly_forecast}")
    conn.execute(query=query_unique_id_hourly_forecast)
    logger.info(f"Query completed.")
    logger.info(f"Running query {query_unique_id_daily_past}")
    conn.execute(query=query_unique_id_daily_past)
    logger.info(f"Query completed.")
    logger.info(f"Running query {query_unique_id_daily_forecast}")
    conn.execute(query_unique_id_daily_forecast)
    logger.info(f"Query completed.")

    conn.close()
