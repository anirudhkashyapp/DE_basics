# Bengaluru Weather ETL Pipeline

A beginner data engineering project that pulls real-time weather data 
for Bengaluru and stores it in a local database for analysis.

## What it does
- Pulls 7 days of Bengaluru weather data from the Open-Meteo API
- Cleans and structures it using Python and pandas
- Loads it into a SQLite database
- Queries it with SQL to answer real questions about the data

## Tech Stack
- Python 3.14
- pandas
- SQLAlchemy
- SQLite
- Open-Meteo API (free, no API key needed)

## Queries answered
- What was the hottest day this week?
- What was the rainiest day this week?
- What were the average min and max temperatures?

## How to run
1. Clone the repo
2. Install dependencies: `pip install requests pandas sqlalchemy`
3. Run the pipeline: `python pipeline.py`
4. Query the data: `python query.py`

## Sample Output
- Hottest day: 2026-04-28 at 36.8°C
- Rainiest day: 2026-04-30 with 6.6mm precipitation
- Avg max temp: 35.49°C | Avg min temp: 23.09°C