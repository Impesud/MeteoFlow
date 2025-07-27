"""'
load_dimensions.py

Imposta e popola le tabelle dimensionali `regions`, `provinces` e `cities` in PostgreSQL.
"""

import logging
import os

import pandas as pd
from sqlalchemy import create_engine, text
from utils.data_utils import clean_dataframe, load_dataframe, pad_istat_code, read_jsonl

# ────────────────────────────────────────────────────────────────────────────────
# Configurazione logging
# ────────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("load_dimensions")


def load_dimensions():
    logger.info("🔌 Inizio caricamento dimensioni")

    # Connection string da env
    db_url = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
    if not db_url:
        logger.error("❌ AIRFLOW__DATABASE__SQL_ALCHEMY_CONN non impostata")
        raise RuntimeError("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN non impostata")

    engine = create_engine(db_url)

    # Percorsi ai file dimensionali
    base = os.getenv("METEO_DATA")
    regions_path = os.path.join(base, "regions.jsonl")
    provinces_path = os.path.join(base, "provinces.jsonl")
    cities_path = os.path.join(base, "cities.csv")

    # Truncate delle tabelle dimensionali
    logger.info("🗑️  Svuoto tabelle dimensions")
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE cities, provinces, regions RESTART IDENTITY CASCADE"))

    # Load Regions
    logger.info("📥 Carico regions da %s", regions_path)
    df_regions = read_jsonl(regions_path)[["region_istat", "region_name", "region_boundaries"]]
    df_regions = clean_dataframe(df_regions, {}, ["region_istat", "region_name"])
    df_regions = pad_istat_code(df_regions, "region_istat", width=6)
    load_dataframe(df_regions, table="regions", engine=engine)

    # Load Provinces
    logger.info("📥 Carico provinces da %s", provinces_path)
    df_provinces = read_jsonl(provinces_path)[["province_istat_code", "province_name", "province_boundaries"]]
    df_provinces = clean_dataframe(df_provinces, {}, ["province_istat_code", "province_name"])
    df_provinces = pad_istat_code(df_provinces, "province_istat_code", width=6)
    load_dataframe(df_provinces, table="provinces", engine=engine)

    # Load Cities
    logger.info("📥 Carico cities da %s", cities_path)
    df_cities = pd.read_csv(
        cities_path,
        sep=";",
        decimal=",",
        dtype={
            "sigla_provincia": str,
            "codice_istat": str,
            "denominazione_ita": str,
            "lat": float,
            "lon": float,
            "superficie_kmq": float,
        },
    )[["codice_istat", "denominazione_ita", "sigla_provincia", "lat", "lon", "superficie_kmq"]]
    df_cities = clean_dataframe(df_cities, {}, ["codice_istat", "denominazione_ita"])
    df_cities = pad_istat_code(df_cities, "codice_istat", width=6)
    load_dataframe(df_cities, table="cities", engine=engine)

    logger.info("🎉 Dimensioni caricate con successo")


if __name__ == "__main__":
    load_dimensions()
