import pandas as pd
from sqlalchemy import create_engine
engine = create_engine("sqlite:///weather.db")
df_monthly = pd.read_sql("""
    SELECT 
        city,
        strftime('%Y-%m', date) as month,
        ROUND(AVG(temp_max), 2) as avg_max,
        ROUND(AVG(precipitation), 2) as avg_rain
    FROM weather
    GROUP BY city, month
    ORDER BY city, month
""", engine)
print("--- Monthly Comparison ---")
print(df_monthly)