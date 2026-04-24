import requests
import pandas as pd


url = "https://api.open-meteo.com/v1/forecast"

params = {
    "latitude": 12.97,      # Bengaluru
    "longitude": 77.59,
    "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
    "timezone": "Asia/Kolkata",
    "past_days": 7          # last 7 days of data
}

response = requests.get(url, params=params)
data = response.json()

df = pd.DataFrame({
    "date": data["daily"]["time"],
    "temp_max": data["daily"]["temperature_2m_max"],
    "temp_min": data["daily"]["temperature_2m_min"],
    "precipitation": data["daily"]["precipitation_sum"]
})

print(df)

from sqlalchemy import create_engine

# Create a local SQLite database file
engine = create_engine("sqlite:///weather.db")

# Store the dataframe into a table called 'bengaluru_weather'
df.to_sql("bengaluru_weather", engine, if_exists="replace", index=False)

print("Data loaded into database successfully!")