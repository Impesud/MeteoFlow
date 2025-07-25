"""
aggregate_sql.py

Orchestra in batch tutte le aggregazioni su weather_city_hourly:

  • città   (daily, weekly, monthly)
  • province
  • regioni

Supporta i flag:
  --scope   city|prov|reg         (obbligatorio)
  --period  daily|weekly|monthly  (facoltativo)

Per le province si unisce weather_city_hourly → cities → provinces.
Per le regioni si unisce weather_city_hourly → cities → regions.
"""

import argparse
import logging
import os
import sys

from sqlalchemy import create_engine, text

# ───────────────────────────────── Logging ────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

# ────────────────────────────── Connessione ─────────────────────────────────
DB_URL = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
if not DB_URL:
    logging.error("❌ AIRFLOW__DATABASE__SQL_ALCHEMY_CONN non impostata")
    sys.exit(1)
engine = create_engine(DB_URL)

# ──────────────────────────── Truncate & Config ─────────────────────────────
TRUNCATES = {
    "city": ["weather_city_daily", "weather_city_weekly", "weather_city_monthly"],
    "prov": ["weather_province_daily", "weather_province_weekly", "weather_province_monthly"],
    "reg": ["weather_region_daily", "weather_region_weekly", "weather_region_monthly"],
}


def infer_period(target: str) -> str:
    if target.endswith("_daily"):
        return "daily"
    if target.endswith("_weekly"):
        return "weekly"
    if target.endswith("_monthly"):
        return "monthly"
    return ""


AGG_CONFIG = [
    # — City daily
    dict(
        scope="city",
        target="weather_city_daily",
        dims=[("DATE(wh.datetime)", "date")],
        metrics=[
            ("AVG(wh.temperature)::float", "avg_temperature"),
            ("AVG(wh.apparent_temperature)::float", "avg_apparent_temperature"),
            ("AVG(wh.dew_point)::float", "avg_dew_point"),
            ("AVG(wh.pressure)::float", "avg_pressure"),
            ("AVG(wh.wind_speed)::float", "avg_wind_speed"),
            ("AVG(wh.cloud_cover)::float", "avg_cloud_cover"),
            ("SUM(wh.precip_intensity)::float", "total_precipitation"),
        ],
        extras=[
            ("MODE() WITHIN GROUP (ORDER BY wh.icon)", "dominant_icon"),
            ("MODE() WITHIN GROUP (ORDER BY wh.summary)", "summary"),
        ],
    ),
    # — City weekly
    dict(
        scope="city",
        target="weather_city_weekly",
        dims=[("EXTRACT(YEAR FROM wh.datetime)::int", "year"), ("date_part('week', wh.datetime)::int", "week")],
        metrics=[
            ("AVG(wh.temperature)::float", "avg_temperature"),
            ("SUM(wh.precip_intensity)::float", "total_precipitation"),
        ],
        extras=[],
    ),
    # — City monthly
    dict(
        scope="city",
        target="weather_city_monthly",
        dims=[("EXTRACT(YEAR FROM wh.datetime)::int", "year"), ("EXTRACT(MONTH FROM wh.datetime)::int", "month")],
        metrics=[
            ("AVG(wh.temperature)::float", "avg_temperature"),
            ("SUM(wh.precip_intensity)::float", "total_precipitation"),
        ],
        extras=[],
    ),
    # — Province daily
    dict(
        scope="prov",
        target="weather_province_daily",
        dims=[("DATE(wh.datetime)", "date")],
        metrics=[
            ("AVG(wh.temperature)::float", "avg_temperature"),
            ("SUM(wh.precip_intensity)::float", "total_precipitation"),
        ],
        extras=[("MODE() WITHIN GROUP (ORDER BY wh.icon)", "dominant_icon")],
    ),
    # — Province weekly
    dict(
        scope="prov",
        target="weather_province_weekly",
        dims=[("EXTRACT(YEAR FROM wh.datetime)::int", "year"), ("date_part('week', wh.datetime)::int", "week")],
        metrics=[
            ("AVG(wh.temperature)::float", "avg_temperature"),
            ("SUM(wh.precip_intensity)::float", "total_precipitation"),
        ],
        extras=[],
    ),
    # — Province monthly
    dict(
        scope="prov",
        target="weather_province_monthly",
        dims=[("EXTRACT(YEAR FROM wh.datetime)::int", "year"), ("EXTRACT(MONTH FROM wh.datetime)::int", "month")],
        metrics=[
            ("AVG(wh.temperature)::float", "avg_temperature"),
            ("SUM(wh.precip_intensity)::float", "total_precipitation"),
        ],
        extras=[],
    ),
    # — Region daily
    dict(
        scope="reg",
        target="weather_region_daily",
        dims=[("DATE(wh.datetime)", "date")],
        metrics=[
            ("AVG(wh.temperature)::float", "avg_temperature"),
            ("SUM(wh.precip_intensity)::float", "total_precipitation"),
        ],
        extras=[("MODE() WITHIN GROUP (ORDER BY wh.icon)", "dominant_icon")],
    ),
    # — Region weekly
    dict(
        scope="reg",
        target="weather_region_weekly",
        dims=[("EXTRACT(YEAR FROM wh.datetime)::int", "year"), ("date_part('week', wh.datetime)::int", "week")],
        metrics=[
            ("AVG(wh.temperature)::float", "avg_temperature"),
            ("SUM(wh.precip_intensity)::float", "total_precipitation"),
        ],
        extras=[],
    ),
    # — Region monthly
    dict(
        scope="reg",
        target="weather_region_monthly",
        dims=[("EXTRACT(YEAR FROM wh.datetime)::int", "year"), ("EXTRACT(MONTH FROM wh.datetime)::int", "month")],
        metrics=[
            ("AVG(wh.temperature)::float", "avg_temperature"),
            ("SUM(wh.precip_intensity)::float", "total_precipitation"),
        ],
        extras=[],
    ),
]


# ────────────────────────── Costruzione dinamica query ─────────────────────────
def build_agg_query(conf: dict) -> str:
    scope, target = conf["scope"], conf["target"]

    if scope == "city":
        # Join su cities direttamente con istat_code
        join_sql = "JOIN cities    AS c ON c.codice_istat   = wh.istat_code"
        key_col = "wh.istat_code"
    elif scope == "prov":
        # Join via cities → provinces usando c.province_istat
        join_sql = (
            "JOIN cities    AS c ON c.codice_istat    = wh.istat_code\n"
            "JOIN provinces AS p ON p.province_istat_code = c.province_istat"
        )
        key_col = "p.province_istat_code"
    else:  # reg
        # Join via cities → regions usando c.region_istat
        join_sql = (
            "JOIN cities  AS c ON c.codice_istat  = wh.istat_code\n"
            "JOIN regions AS r ON r.region_istat   = c.region_istat"
        )
        key_col = "r.region_istat"

    # prepara SELECT & GROUP BY
    select_parts = [f"{key_col} AS {key_col.split('.')[-1]}"]
    group_by_keys = [key_col]
    for expr, alias in conf["dims"]:
        select_parts.append(f"{expr} AS {alias}")
        group_by_keys.append(alias)
    for expr, alias in conf["metrics"] + conf["extras"]:
        select_parts.append(f"{expr} AS {alias}")

    select_sql = ",\n  ".join(select_parts)
    group_sql = ", ".join(group_by_keys)
    insert_cols = (
        [c.split(".")[-1] for c in group_by_keys]
        + [alias for _, alias in conf["metrics"]]
        + [alias for _, alias in conf["extras"]]
    )

    return f"""
INSERT INTO {target} ({', '.join(insert_cols)})
SELECT
  {select_sql}
FROM weather_city_hourly AS wh
{join_sql}
GROUP BY {group_sql};
"""  # nosec B608


# ───────────────────────────────── Esecuzione ─────────────────────────────────
def main():
    # ─────────────────────────────── CLI Args ────────────────────────────────────
    parser = argparse.ArgumentParser(description="Esegui aggregazioni SQL su meteo")
    parser.add_argument(
        "--scope", choices=["city", "prov", "reg"], required=True, help="Ambito di aggregazione: city, prov o reg"
    )
    parser.add_argument(
        "--period", choices=["daily", "weekly", "monthly"], help="Periodo di aggregazione: daily, weekly o monthly"
    )
    args = parser.parse_args()

    logging.info("▶️  Avvio aggregazioni SQL")

    # filtra per scope e period
    to_do = [
        c
        for c in AGG_CONFIG
        if c["scope"] == args.scope and (args.period is None or infer_period(c["target"]) == args.period)
    ]
    if not to_do:
        logging.warning("⚠️  Nessuna aggregazione da eseguire (filtro troppo restrittivo)")
        sys.exit(0)

    with engine.begin() as conn:
        # 1) TRUNCATE delle tabelle richieste
        scopes = {c["scope"] for c in to_do}
        for s in scopes:
            for tbl in TRUNCATES[s]:
                logging.info(f"  • TRUNCATE {tbl}")
                conn.execute(text(f"TRUNCATE TABLE {tbl} CASCADE"))

        # 2) INSERT ... SELECT per ciascuna conf
        for conf in to_do:
            per = infer_period(conf["target"])
            logging.info(f"  • Popolazione {conf['target']} ({conf['scope']}/{per})")
            conn.execute(text(build_agg_query(conf)))

    logging.info("🎉 Aggregazioni completate con successo!")


if __name__ == "__main__":
    main()
