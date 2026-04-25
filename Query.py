import pandas as pd
from sqlalchemy import create_engine
engine = create_engine("sqlite:///weather.db")

df_temp1 = pd.read_sql("""
    SELECT 
    w.city, 
    ROUND(AVG(w.temp_min), 2) as temp_min
FROM weather w
JOIN cities c ON w.city = c.city
GROUP BY w.city
ORDER BY temp_min ASC
""",engine)
print("--- minimum average temperature ---")
print(df_temp1)