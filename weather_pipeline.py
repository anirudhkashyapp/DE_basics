from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from sqlalchemy import create_engine, text

def fetch_and_load_weather():
    engine = create_engine("sqlite:////home/anirudh/weather.db")

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
        return pd.DataFrame({
            "city": city["name"],
            "date": data["daily"]["time"],
            "temp_max": data["daily"]["temperature_2m_max"],
            "temp_min": data["daily"]["temperature_2m_min"],
            "precipitation": data["daily"]["precipitation_sum"]
        })

    all_data = pd.concat([fetch_weather(city) for city in cities])
    
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS weather"))
        conn.execute(text("""
            CREATE TABLE weather (
                city TEXT,
                date TEXT,
                temp_max REAL,
                temp_min REAL,
                precipitation REAL
            )
        """))
        for _, row in all_data.iterrows():
            conn.execute(text(
                "INSERT INTO weather VALUES (:city, :date, :temp_max, :temp_min, :precipitation)"
            ), {
                "city": row["city"],
                "date": row["date"],
                "temp_max": row["temp_max"],
                "temp_min": row["temp_min"],
                "precipitation": row["precipitation"]
            })

default_args = {
    "owner": "anirudh",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    "weather_pipeline",
    default_args=default_args,
    description="Fetch weather data for Indian cities daily",
    schedule_interval="@daily",
    start_date=datetime(2026, 4, 27),
    catchup=False
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_and_load_weather",
        python_callable=fetch_and_load_weather
    )
