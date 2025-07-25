-- Verifica che esistano aggregazioni giornaliere per città
SELECT COUNT(*)
FROM (
  SELECT date FROM weather_city_daily GROUP BY date
) AS sub;