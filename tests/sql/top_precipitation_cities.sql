-- Elenca le 5 città con maggior precipitazione totale nell'ultimo giorno disponibile
WITH latest AS (
SELECT MAX(date) AS d FROM weather_city_daily
)
SELECT wcd.istat_code, c.denominazione_ita, wcd.total_precipitation
FROM weather_city_daily wcd
JOIN cities c ON c.codice_istat = wcd.istat_code
JOIN latest ON wcd.date = latest.d
ORDER BY wcd.total_precipitation DESC
LIMIT 5;