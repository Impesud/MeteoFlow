-- Calcola la media mobile su 7 giorni dell'avg_temperature per una città (es. primo ISTAT trovato)
WITH city_list AS (
SELECT DISTINCT istat_code FROM weather_city_daily LIMIT 1
),
-- Rolling window: 7 giorni precedenti
rolling AS (
SELECT
wcd.date,
wcd.istat_code,
AVG(wcd.avg_temperature) OVER (
PARTITION BY wcd.istat_code
ORDER BY wcd.date
ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
) AS rolling_avg_temp
FROM weather_city_daily wcd
JOIN city_list cl ON cl.istat_code = wcd.istat_code
)
SELECT date, istat_code, rolling_avg_temp
FROM rolling
ORDER BY date DESC
LIMIT 7;