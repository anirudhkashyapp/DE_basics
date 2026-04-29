SELECT
    city,
    ROUND(AVG(temp_max), 2) as avg_max_temp,
    ROUND(AVG(temp_min), 2) as avg_min_temp,
    ROUND(AVG(temp_range), 2) as avg_temp_range,
    ROUND(SUM(precipitation), 2) as total_rainfall,
    MAX(temp_max) as hottest_day_temp,
    MIN(temp_min) as coldest_night_temp
FROM {{ ref('stg_weather') }}
GROUP BY city
ORDER BY avg_max_temp DESC
