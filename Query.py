import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("sqlite:///weather.db")

# Query 1 - see all the data
df = pd.read_sql("SELECT * FROM bengaluru_weather", engine)
print("--- All Data ---")
print(df)

# Query 2 - hottest day this week
df2 = pd.read_sql("""
    SELECT date, temp_max 
    FROM bengaluru_weather 
    ORDER BY temp_max DESC 
    LIMIT 1
""", engine)
print("\n--- Hottest Day ---")
print(df2)

# Query 3 - average min and max temperature
df3 = pd.read_sql("""
    SELECT 
        ROUND(AVG(temp_max), 2) as avg_max,
        ROUND(AVG(temp_min), 2) as avg_min
    FROM bengaluru_weather
""", engine)
print("\n--- Average Temperatures ---")
print(df3)

df4 = pd.read_sql("""
    SELECT date, precipitation 
    FROM bengaluru_weather 
    ORDER BY precipitation DESC 
    LIMIT 1
""", engine)
print("\n--- Rainiest Day ---")
print(df4)