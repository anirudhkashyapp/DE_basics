import pandas as pd
from sqlalchemy import create_engine
engine = create_engine("sqlite:///weather.db")

df_range = pd.read_sql("""
    SELECT w.city, 
        MAX(w.temp_max) as max_temp, 
        MIN(w.temp_min) as min_temp,
        ROUND(MAX(w.temp_max) - MIN(w.temp_min), 2) as temp_range
    FROM weather w
    JOIN cities c ON w.city = c.city
    GROUP BY w.city
    ORDER BY temp_range DESC
""", engine)
print("--- Temperature Range by City ---")
print(df_range)