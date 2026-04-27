import pandas as pd
from sqlalchemy import create_engine
engine = create_engine("sqlite:///weather.db")

df_temp4 = pd.read_sql("""
  SELECT w.city, ROUND(SUM(w.precipitation),2) as total_rain
FROM weather w
JOIN cities c ON w.city = c.city
GROUP BY w.city
HAVING total_rain>10
ORDER BY total_rain DESC
 """, engine)
print(df_temp4)