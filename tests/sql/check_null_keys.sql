-- Controlla che non ci siano chiavi ISTAT null
SELECT *
FROM (
SELECT istat_code FROM weather_city_daily
UNION ALL
SELECT province_istat_code FROM weather_province_daily
UNION ALL
SELECT region_istat FROM weather_region_daily
) AS keys
WHERE keys IS NULL;