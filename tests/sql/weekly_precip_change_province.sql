-- Confronta precipitazione totale tra l'ultima e la penultima settimana per una provincia (es. primo codice)
WITH prov_list AS (
  SELECT DISTINCT province_istat_code
    FROM weather_province_weekly
   LIMIT 1
), weeks AS (
  SELECT
    wpw.year,
    wpw.week,
    wpw.total_precipitation,
    ROW_NUMBER() OVER (ORDER BY wpw.year DESC, wpw.week DESC) AS rn
  FROM weather_province_weekly wpw
  JOIN prov_list pl 
    ON pl.province_istat_code = wpw.province_istat_code
)
SELECT
  w1.year                AS year_curr,
  w1.week                AS week_curr,
  w1.total_precipitation AS precip_curr,
  w2.year                AS year_prev,
  w2.week                AS week_prev,
  w2.total_precipitation AS precip_prev,
  ROUND(
    ((w1.total_precipitation - w2.total_precipitation)
      / NULLIF(w2.total_precipitation,0)
    )::numeric,
    2
  ) AS pct_change
FROM weeks w1
LEFT JOIN weeks w2 
  ON w2.rn = w1.rn + 1
WHERE w1.rn = 1;
