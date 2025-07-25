-- Verifica che per ogni città e data ci siano 24 record orari
SELECT date_trunc('day', datetime) AS day, istat_code, COUNT() AS hourly_count
FROM weather_city_hourly
GROUP BY day, istat_code
HAVING COUNT() <> 24
LIMIT 10;