import requests
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("sqlite:///weather.db")

cities = [
    {"name": "Bengaluru", "latitude": 12.97, "longitude": 77.59},
    {"name": "Mumbai", "latitude": 19.07, "longitude": 72.87}
]

def fetch_weather(city):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
        "timezone": "Asia/Kolkata",
        "past_days": 30
    }
    response = requests.get(url, params=params)
    data = response.json()

  # Create a cities reference table
cities_info = pd.DataFrame({
    "city": ["Bengaluru", "Mumbai"],
    "state": ["Karnataka", "Maharashtra"],
    "population": [13000000, 20000000]
})

cities_info.to_sql("cities", engine, if_exists="replace", index=False)
print("Cities table loaded!")