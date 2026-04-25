import pandas as pd
from sqlalchemy import create_engine
engine = create_engine("sqlite:///weather.db")

df_temp2 = pd.read_sql("""
    SELECT w.city, w.date,
    ROUND(MAX(precipitation), 2) as max_rain
FROM weather w
JOIN cities c ON w.city = c.city
ORDER BY max_rain DESC
LIMIT 1 """, engine)
print(df_temp2)