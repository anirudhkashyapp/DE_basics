import pandas as pd
from sqlalchemy import create_engine
engine = create_engine("sqlite:///weather.db")

df_temp3 = pd.read_sql("""
  SELECT w.city, w.date, MAX(w.temp_max) as hottest_day
FROM weather w
JOIN cities c ON w.city = c.city
GROUP BY w.city
ORDER BY hottest_day DESC
 """, engine)
print(df_temp3)