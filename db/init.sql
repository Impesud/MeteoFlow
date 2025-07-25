-- ============================================
-- METEOFLOW – INIT SQL 
-- ============================================

-- 1️⃣ Aggiunto il supporto PostGIS
CREATE EXTENSION IF NOT EXISTS postgis;

-- ───────────────────────────────────────────
-- 🟪 Tabella Regioni
-- File: regions.jsonl
-- Campi: region_istat, region_name, region_boundaries
-- ───────────────────────────────────────────
DROP TABLE IF EXISTS regions CASCADE;
CREATE TABLE regions (
    region_istat       VARCHAR(10) PRIMARY KEY,
    region_name        VARCHAR(100) NOT NULL,
    region_boundaries  TEXT        NOT NULL    -- WKT multipolygon
);

-- ───────────────────────────────────────────
-- 🟧 Tabella Province
-- File: provinces.jsonl
-- Campi: province_istat_code, province_name, province_boundaries
-- ───────────────────────────────────────────
DROP TABLE IF EXISTS provinces CASCADE;
CREATE TABLE provinces (
    province_istat_code   VARCHAR(10) PRIMARY KEY,
    province_name         VARCHAR(100) NOT NULL,
    province_boundaries   TEXT        NOT NULL    -- WKT multipolygon
);

-- ───────────────────────────────────────────
-- 🟨 Tabella Città (Comuni)
-- File: cities.csv
-- Campi: codice_istat, denominazione_ita, sigla_provincia, lat, lon, superficie_kmq
-- ───────────────────────────────────────────
DROP TABLE IF EXISTS cities CASCADE;
CREATE TABLE cities (
    codice_istat       VARCHAR(10) PRIMARY KEY,
    denominazione_ita  VARCHAR(100) NOT NULL,
    sigla_provincia    VARCHAR(5),
    lat                DOUBLE PRECISION,
    lon                DOUBLE PRECISION,
    superficie_kmq     DOUBLE PRECISION
);

-- ───────────────────────────────────────────
-- 🌧️ Raw Meteo Orari
-- (dati orari estratti da hourly_weather.csv)
-- ───────────────────────────────────────────
DROP TABLE IF EXISTS weather_city_hourly CASCADE;
CREATE TABLE weather_city_hourly (
    istat_code      VARCHAR(10) REFERENCES cities(codice_istat),
    datetime             TIMESTAMP NOT NULL,
    temperature          FLOAT,
    apparent_temperature FLOAT,
    dew_point            FLOAT,
    pressure             FLOAT,
    wind_speed           FLOAT,
    wind_gust            FLOAT,
    wind_bearing         FLOAT,
    cloud_cover          FLOAT,
    precip_intensity     FLOAT,
    precip_accumulation  FLOAT,
    precip_type          VARCHAR(10),
    snow_accumulation    FLOAT,
    icon                 VARCHAR(50),
    summary              TEXT
);

-- ───────────────────────────────────────────
-- 📅 Dati Giornalieri per Città
-- aggregati da weather_hourly via SQL
-- ───────────────────────────────────────────
DROP TABLE IF EXISTS weather_city_daily CASCADE;
CREATE TABLE weather_city_daily (
    id                       SERIAL PRIMARY KEY,
    istat_code               VARCHAR(10) REFERENCES cities(codice_istat),
    date                     DATE    NOT NULL,
    avg_temperature          FLOAT,
    avg_apparent_temperature FLOAT,
    avg_dew_point            FLOAT,
    avg_pressure             FLOAT,
    avg_wind_speed           FLOAT,
    avg_cloud_cover          FLOAT,
    total_precipitation      FLOAT,
    dominant_icon            VARCHAR(50),
    summary                  TEXT
);

-- ───────────────────────────────────────────
-- 📆 Dati Settimanali per Città
-- ───────────────────────────────────────────
DROP TABLE IF EXISTS weather_city_weekly CASCADE;
CREATE TABLE weather_city_weekly (
    id                       SERIAL PRIMARY KEY,
    istat_code               VARCHAR(10) REFERENCES cities(codice_istat),
    year                     INTEGER NOT NULL,
    week                     INTEGER NOT NULL,
    avg_temperature          FLOAT,
    avg_apparent_temperature FLOAT,
    avg_dew_point            FLOAT,
    avg_pressure             FLOAT,
    avg_wind_speed           FLOAT,
    avg_cloud_cover          FLOAT,
    total_precipitation      FLOAT
);

-- ───────────────────────────────────────────
-- 🗓️ Dati Mensili per Città
-- ───────────────────────────────────────────
DROP TABLE IF EXISTS weather_city_monthly CASCADE;
CREATE TABLE weather_city_monthly (
    id                       SERIAL PRIMARY KEY,
    istat_code               VARCHAR(10) REFERENCES cities(codice_istat),
    year                     INTEGER NOT NULL,
    month                    INTEGER NOT NULL,
    avg_temperature          FLOAT,
    avg_apparent_temperature FLOAT,
    avg_dew_point            FLOAT,
    avg_pressure             FLOAT,
    avg_wind_speed           FLOAT,
    avg_cloud_cover          FLOAT,
    total_precipitation      FLOAT
);

-- ───────────────────────────────────────────
-- 🟩 Dati Giornalieri per Provincia
-- aggregati da weather_daily o via SQL su weather_hourly
-- ───────────────────────────────────────────
DROP TABLE IF EXISTS weather_province_daily CASCADE;
CREATE TABLE weather_province_daily (
    id                       SERIAL PRIMARY KEY,
    province_istat_code      VARCHAR(10) REFERENCES provinces(province_istat_code),
    date                     DATE    NOT NULL,
    avg_temperature          FLOAT,
    total_precipitation      FLOAT,
    dominant_icon            VARCHAR(50)
);

-- ───────────────────────────────────────────
-- 🟩 Dati Settimanali per Provincia
-- ───────────────────────────────────────────
DROP TABLE IF EXISTS weather_province_weekly CASCADE;
CREATE TABLE weather_province_weekly (
    id                       SERIAL PRIMARY KEY,
    province_istat_code      VARCHAR(10) REFERENCES provinces(province_istat_code),
    year                     INTEGER NOT NULL,
    week                     INTEGER NOT NULL,
    avg_temperature          FLOAT,
    total_precipitation      FLOAT
);

-- ───────────────────────────────────────────
-- 🟩 Dati Mensili per Provincia
-- ───────────────────────────────────────────
DROP TABLE IF EXISTS weather_province_monthly CASCADE;
CREATE TABLE weather_province_monthly (
    id                       SERIAL PRIMARY KEY,
    province_istat_code      VARCHAR(10) REFERENCES provinces(province_istat_code),
    year                     INTEGER NOT NULL,
    month                    INTEGER NOT NULL,
    avg_temperature          FLOAT,
    total_precipitation      FLOAT
);

-- ───────────────────────────────────────────
-- 🟦 Dati Giornalieri per Regione
-- aggregati da weather_province o via SQL su weather_hourly
-- ───────────────────────────────────────────
DROP TABLE IF EXISTS weather_region_daily CASCADE;
CREATE TABLE weather_region_daily (
    id                       SERIAL PRIMARY KEY,
    region_istat             VARCHAR(10) REFERENCES regions(region_istat),
    date                     DATE    NOT NULL,
    avg_temperature          FLOAT,
    total_precipitation      FLOAT,
    dominant_icon            VARCHAR(50)
);

-- ───────────────────────────────────────────
-- 🟦 Dati Settimanali per Regione
-- ───────────────────────────────────────────
DROP TABLE IF EXISTS weather_region_weekly CASCADE;
CREATE TABLE weather_region_weekly (
    id                       SERIAL PRIMARY KEY,
    region_istat             VARCHAR(10) REFERENCES regions(region_istat),
    year                     INTEGER NOT NULL,
    week                     INTEGER NOT NULL,
    avg_temperature          FLOAT,
    total_precipitation      FLOAT
);

-- ───────────────────────────────────────────
-- 🟦 Dati Mensili per Regione
-- ───────────────────────────────────────────
DROP TABLE IF EXISTS weather_region_monthly CASCADE;
CREATE TABLE weather_region_monthly (
    id                       SERIAL PRIMARY KEY,
    region_istat             VARCHAR(10) REFERENCES regions(region_istat),
    year                     INTEGER NOT NULL,
    month                    INTEGER NOT NULL,
    avg_temperature          FLOAT,
    total_precipitation      FLOAT
);

-- Clona la struttura di weather_city_hourly
CREATE TABLE IF NOT EXISTS weather_city_hourly_tmp (LIKE weather_city_hourly INCLUDING ALL);

-- Aggiungi il vincolo di unicità su (istat_code, datetime)
ALTER TABLE weather_city_hourly
  ADD CONSTRAINT uq_city_datetime
    UNIQUE (istat_code, datetime);