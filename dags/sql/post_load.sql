-- ----------------------------------------------------------------------------- 
-- post_load.sql (esteso) 
-- ----------------------------------------------------------------------------- 

-- 1) conversione WKT → GEOMETRY
ALTER TABLE regions
  ADD COLUMN geom GEOMETRY(MULTIPOLYGON, 4326);
UPDATE regions
  SET geom = ST_GeomFromText(region_boundaries, 4326);

ALTER TABLE provinces
  ADD COLUMN geom GEOMETRY(MULTIPOLYGON, 4326);
UPDATE provinces
  SET geom = ST_GeomFromText(province_boundaries, 4326);

-- 2) aggiunta di region_istat in cities
ALTER TABLE cities
  ADD COLUMN region_istat VARCHAR(10);
UPDATE cities AS c
  SET region_istat = r.region_istat
FROM regions AS r
WHERE ST_Contains(
    r.geom,
    ST_SetSRID(ST_MakePoint(c.lon, c.lat), 4326)
);

-- 3) aggiunta di province_istat in cities
ALTER TABLE cities
  ADD COLUMN province_istat VARCHAR(10);

UPDATE cities AS c
  SET province_istat = p.province_istat_code
FROM provinces AS p
WHERE ST_Contains(
    p.geom,
    ST_SetSRID(ST_MakePoint(c.lon, c.lat), 4326)
);

-- 4) indici opzionali per performance sui join futuri
-- CREATE INDEX ON cities (region_istat);
-- CREATE INDEX ON cities (province_istat);

-- 5) cleanup: rimuovi colonne WKT se non più servono
-- ALTER TABLE regions   DROP COLUMN region_boundaries;
-- ALTER TABLE provinces DROP COLUMN province_boundaries;


