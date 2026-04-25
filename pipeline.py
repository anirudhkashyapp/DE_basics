import requests
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("sqlite:///weather.db")

cities = [
    {"name": "Bengaluru", "latitude": 12.97, "longitude": 77.59},
    {"name": "Mumbai", "latitude": 19.07, "longitude": 72.87},
    {"name": "Delhi", "latitude": 28.61, "longitude": 77.21},
    {"name": "Chennai", "latitude": 13.08, "longitude": 80.27}
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

    df = pd.DataFrame({
        "city": city["name"],
        "date": data["daily"]["time"],
        "temp_max": data["daily"]["temperature_2m_max"],
        "temp_min": data["daily"]["temperature_2m_min"],
        "precipitation": data["daily"]["precipitation_sum"]
    })
    return df

all_data = pd.concat([fetch_weather(city) for city in cities])
all_data.to_sql("weather", engine, if_exists="replace", index=False)
print("Weather data loaded!")
print(all_data.head(10))

cities_info = pd.DataFrame({
    "city": ["Bengaluru", "Mumbai", "Delhi", "Chennai"],
    "state": ["Karnataka", "Maharashtra", "Delhi", "Tamil Nadu"],
    "population": [13000000, 20000000, 33000000, 12000000]
})

cities_info.to_sql("cities", engine, if_exists="replace", index=False)
print("Cities table loaded!")