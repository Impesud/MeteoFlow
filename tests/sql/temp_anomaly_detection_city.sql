-- Rileva anomalie di temperatura giornaliera per una città:
-- Z-score > 2 su media mobile di 30 giorni
WITH city AS (
  SELECT DISTINCT istat_code
  FROM weather_city_daily
  LIMIT 1
), daily_stats AS (
  SELECT
    w.date,
    w.istat_code,
    w.avg_temperature,
    AVG(w.avg_temperature) OVER (
      PARTITION BY w.istat_code
      ORDER BY w.date
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS moving_avg_30d,
    STDDEV(w.avg_temperature) OVER (
      PARTITION BY w.istat_code
      ORDER BY w.date
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS moving_std_30d
  FROM weather_city_daily w
  JOIN city c ON c.istat_code = w.istat_code
)
SELECT
  date,
  istat_code,
  ROUND(
    ((avg_temperature - moving_avg_30d)
      / NULLIF(moving_std_30d,0)
    )::numeric,
    2
  ) AS z_score,
  avg_temperature,
  moving_avg_30d
FROM daily_stats
WHERE moving_std_30d > 0
  AND ABS(
    (avg_temperature - moving_avg_30d) / moving_std_30d
  ) > 2
ORDER BY date DESC
LIMIT 10;
