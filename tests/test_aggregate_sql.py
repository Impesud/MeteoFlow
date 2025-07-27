import pytest

from src.scripts.aggregate_sql import AGG_CONFIG, build_agg_query, infer_period


# --- infer_period tests ---
def test_infer_period_suffix_daily():
    assert infer_period("weather_city_daily") == "daily"


def test_infer_period_suffix_weekly():
    assert infer_period("weather_city_weekly") == "weekly"


def test_infer_period_suffix_monthly():
    assert infer_period("weather_city_monthly") == "monthly"


def test_infer_period_invalid():
    assert infer_period("weather_city_hourly") == ""


# --- build_agg_query tests ---
@pytest.mark.parametrize(
    "scope,target_key,join_snippet,key_alias",
    [
        (
            "city",
            "weather_city_daily",
            "JOIN cities    AS c ON c.codice_istat   = wh.istat_code",
            "istat_code",
        ),
        (
            "prov",
            "weather_province_weekly",
            "JOIN cities    AS c ON c.codice_istat    = wh.istat_code\n"
            "JOIN provinces AS p ON p.province_istat_code = c.province_istat",
            "province_istat_code",
        ),
        (
            "reg",
            "weather_region_monthly",
            "JOIN cities  AS c ON c.codice_istat  = wh.istat_code\n"
            "JOIN regions AS r ON r.region_istat   = c.region_istat",
            "region_istat",
        ),
    ],
)
def test_build_query_structure(scope, target_key, join_snippet, key_alias):
    # find matching config
    conf = next(
        (c for c in AGG_CONFIG if c["scope"] == scope and c["target"] == target_key),
        None,
    )
    assert conf, f"Config missing for {scope}/{target_key}"

    q = build_agg_query(conf)

    # check INSERT INTO
    assert q.strip().upper().startswith(f"INSERT INTO {target_key}".upper()), "Missing INSERT INTO"

    # FROM & SELECT
    assert "FROM weather_city_hourly AS wh" in q, "Missing FROM clause"
    assert "SELECT" in q, "Missing SELECT clause"

    # JOIN snippet
    assert join_snippet in q, f"Missing JOIN snippet:\n{join_snippet}"

    # GROUP BY
    assert "GROUP BY" in q, "Missing GROUP BY"

    # key alias in insert columns
    assert key_alias in q, f"Key alias '{key_alias}' not in INSERT columns"


def test_dimensions_metrics_extras():
    # pick city-daily for detailed checks
    conf = next(c for c in AGG_CONFIG if c["target"] == "weather_city_daily")
    q = build_agg_query(conf)

    # dimension
    assert "DATE(wh.datetime) AS date" in q

    # metric
    assert "AVG(wh.temperature)::float AS avg_temperature" in q

    # extra
    assert "MODE() WITHIN GROUP (ORDER BY wh.icon) AS dominant_icon" in q
