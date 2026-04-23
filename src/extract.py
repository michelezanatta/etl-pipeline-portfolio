import requests
import pandas as pd

def fetch_weather(latitude=41.90, longitude=12.49, days=7):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": "temperature_2m,precipitation,windspeed_10m",
        "timezone": "Europe/Rome",
        "past_days": days
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    df = pd.DataFrame(data["hourly"])
    df["time"] = pd.to_datetime(df["time"])
    return df

if __name__ == "__main__":
    df = fetch_weather()
    print(df.head())