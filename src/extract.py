import requests
import pandas as pd
import logging
from datetime import datetime

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def define_type(time, today):
    if time <= today:
        return "past"
    else:
        return "forecast"

def fetch_weather(latitude=41.90, longitude=12.49, days=7):
    url = "https://api.open-meteo.com/v1/forecast"
    today = datetime.today()
    
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": "temperature_2m,precipitation,windspeed_10m",
        "daily": ["sunset", "sunrise", "daylight_duration"],
        "timezone": "Europe/Rome",
        "past_days": days,
        "forecast_days": days
    }

    logger.info(f"Fetching weather data for latitude: {latitude}, longitude: {longitude}, days: {days}")
    logger.debug(f"Request URL: {url}")
    logger.debug(f"Request parameters: {params}")

    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    
    df_hourly = pd.DataFrame(data["hourly"])
    df_hourly["latitude"] = latitude
    df_hourly["longitude"] = longitude
    df_hourly["time"] = pd.to_datetime(df_hourly["time"])
    df_hourly["type_measurement"] = df_hourly["time"].apply(lambda x: define_type(x, today))
    df_hourly["time_request"] = today
    df_hourly_past = df_hourly[df_hourly["type_measurement"] == "past"]
    df_hourly_past = df_hourly_past.drop(columns = ["type_measurement"])
    logger.debug(f"Hourly past data: {df_hourly_past.head()}")
    df_hourly_forecast = df_hourly[df_hourly["type_measurement"] == "forecast"]
    df_hourly_forecast = df_hourly_forecast.drop(columns = ["type_measurement"])
    logger.debug(f"Hourly forecast data: {df_hourly_forecast.head()}")

    df_daily = pd.DataFrame(data["daily"])
    df_daily["latitude"] = latitude
    df_daily["longitude"] = longitude
    df_daily['time'] = pd.to_datetime(df_daily["time"])
    df_daily["type_measurement"] = df_daily["time"].apply(lambda x: define_type(x, today))
    df_daily["time_request"] = today
    df_daily_past = df_daily[df_daily["type_measurement"] == "past"]
    df_daily_past = df_daily_past.drop(columns = ["type_measurement"])
    logger.debug(f"Daily past data: {df_daily_past.head()}")
    df_daily_forecast = df_daily[df_daily["type_measurement"] == "forecast"]
    df_daily_forecast = df_daily_forecast.drop(columns = ["type_measurement"])
    logger.debug(f"Daily forecast data: {df_daily_forecast.head()}")

    logger.info("Weather data fetched and processed into DataFrames")

    return df_hourly_past, df_hourly_forecast, df_daily_past, df_daily_forecast

if __name__ == "__main__":
    logger.info("Starting weather data extraction")
    df_hourly_past, df_hourly_forecast, df_daily_past, df_daily_forecast = fetch_weather()
    logger.info("Weather data extraction completed successfully")