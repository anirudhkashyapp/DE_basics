import pandas as pd
from sqlalchemy import create_engine
engine = create_engine("sqlite:///weather.db")

df_temp34 = pd.read_sql("""
    SELECT 
    w.city, 
    ROUND(AVG(w.temp_max), 2) as avg_max
FROM weather w
JOIN cities c ON w.city = c.city
GROUP BY w.city
HAVING AVG(w.temp_max) > 34
ORDER BY avg_max DESC
""",engine)
print("--- temperature greater than 34 ---")
print(df_temp34)