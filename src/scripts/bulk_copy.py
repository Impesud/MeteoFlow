"""
Scarica i dati orari CSV generati da `extract.py` e li importa in blocco
nella tabella di destinazione Postgres usando COPY via psycopg2 con upsert
per evitare duplicati.

La configurazione viene letta da:
- `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`
- `METEO_CSV_PATH`
- `METEO_TABLE`

Il CSV di input può avere più colonne di quelle necessarie: qui ne estraiamo
solo quelle mappate in `HOURLY_COLUMNS` e le riscriviamo in un file temporaneo
per poi usarlo con `COPY … STDIN` e infine fare l’upsert.

"""

import csv
import logging
import os
import sys
import tempfile
from pathlib import Path

import psycopg2
from psycopg2 import sql

# ────── CONFIG & LOGGING ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

DB_URL = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
CSV_PATH = Path(os.getenv("METEO_CSV_PATH", ""))
TABLE = os.getenv("METEO_TABLE")

if not DB_URL or not CSV_PATH or not TABLE:
    logging.error("❌ ENV variables AIRFLOW__DATABASE__SQL_ALCHEMY_CONN, METEO_CSV_PATH o METEO_TABLE non impostate")
    sys.exit(1)

# mapping header CSV → colonna target
COLUMNS_RENAME = {
    "istat_code": "istat_code",
    "datetime": "datetime",
    "temperature": "temperature",
    "apparentTemperature": "apparent_temperature",
    "dewPoint": "dew_point",
    "pressure": "pressure",
    "windSpeed": "wind_speed",
    "windGust": "wind_gust",
    "windBearing": "wind_bearing",
    "cloudCover": "cloud_cover",
    "precipIntensity": "precip_intensity",
    "precipAccumulation": "precip_accumulation",
    "precipType": "precip_type",
    "snowAccumulation": "snow_accumulation",
    "icon": "icon",
    "summary": "summary",
}

HOURLY_COLUMNS = [
    "istat_code",
    "datetime",
    "temperature",
    "apparent_temperature",
    "dew_point",
    "pressure",
    "wind_speed",
    "wind_gust",
    "wind_bearing",
    "cloud_cover",
    "precip_intensity",
    "precip_accumulation",
    "precip_type",
    "snow_accumulation",
    "icon",
    "summary",
]


# ────── HELPERS ──────────────────────────────────────────────────────────
def get_raw_conn(dsn: str):
    """Restituisce una connessione psycopg2 a partire da DSN sqlalchemy-like."""
    if dsn.startswith("postgresql+psycopg2://"):
        dsn = dsn.replace("postgresql+psycopg2://", "postgresql://", 1)
    return psycopg2.connect(dsn)


# ────── MAIN ─────────────────────────────────────────────────────────────
def bulk_copy():
    if not CSV_PATH.exists():
        logging.error(f"❌ CSV non trovato: {CSV_PATH!s}")
        sys.exit(1)

    rev_map = {v: k for k, v in COLUMNS_RENAME.items()}

    # 1) Creiamo un file temporaneo con solo le colonne utili
    with tempfile.TemporaryDirectory(prefix="bulk_copy_") as tmpdir:
        tmp_csv = Path(tmpdir) / "hourly_subset.csv"
        logging.info("▶️  Costruisco CSV temporaneo con colonne filtri…")

        with CSV_PATH.open(newline="", encoding="utf-8") as src, tmp_csv.open("w", newline="", encoding="utf-8") as dst:
            reader = csv.DictReader(src)
            missing = [src_col for src_col in rev_map.values() if src_col not in reader.fieldnames]
            if missing:
                logging.error(f"❌ Colonne mancanti nel CSV di origine: {missing}")
                sys.exit(1)

            writer = csv.writer(dst)
            writer.writerow(HOURLY_COLUMNS)

            for row in reader:
                out = []
                for tgt in HOURLY_COLUMNS:
                    src_col = rev_map.get(tgt, tgt)
                    out.append(row.get(src_col, ""))
                writer.writerow(out)

        logging.info("▶️  Bulk‑loading in staging weather_city_hourly_tmp…")
        conn = get_raw_conn(DB_URL)
        try:
            with conn.cursor() as cur, tmp_csv.open("r", encoding="utf-8") as f:
                # 2) Carica in staging table
                copy_stmt = sql.SQL(
                    """
                    COPY weather_city_hourly_tmp ({fields})
                    FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',')
                    """
                ).format(fields=sql.SQL(", ").join(map(sql.Identifier, HOURLY_COLUMNS)))
                cur.copy_expert(copy_stmt.as_string(cur), f)

                # 3) Upsert in tabella definitiva
                insert_stmt = sql.SQL(
                    """
                    INSERT INTO weather_city_hourly ({fields})
                      SELECT {fields} FROM weather_city_hourly_tmp
                    ON CONFLICT (istat_code, datetime) DO NOTHING
                    """
                ).format(fields=sql.SQL(", ").join(map(sql.Identifier, HOURLY_COLUMNS)))
                cur.execute(insert_stmt)

                # 4) Pulisci la staging table
                cur.execute("TRUNCATE TABLE weather_city_hourly_tmp")

            conn.commit()
            logging.info("✔️  Bulk load e upsert completati con successo")
        except Exception:
            conn.rollback()
            logging.exception("❌ Bulk load/upsert fallito")
            sys.exit(1)
        finally:
            conn.close()


if __name__ == "__main__":
    bulk_copy()
