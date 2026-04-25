import pandas as pd
from sqlalchemy import create_engine
engine = create_engine("sqlite:///weather.db")

df_rain = pd.read_sql("""
    SELECT 
        c.state,
        ROUND(AVG(w.precipitation), 2) as avg_rain
    FROM weather w
    JOIN cities c ON w.city = c.city
    GROUP BY c.state
    ORDER BY avg_rain DESC
""", engine)
print("--- Rainfall by State ---")
print(df_rain)