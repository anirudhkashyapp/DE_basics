SELECT
    city,
    date,
    temp_max,
    temp_min,
    precipitation,
    ROUND(temp_max - temp_min, 2) as temp_range
FROM main.weather
