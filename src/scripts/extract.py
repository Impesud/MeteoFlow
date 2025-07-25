"""
extract.py

Estrae da una directory di file JSONL (.jsonl o .jsonl.gz)
i dati orari meteo, li normalizza e li esporta in un unico CSV.

Variabili d’ambiente richieste:
  - METEO_INPUT_DIR:  directory con i file *.jsonl[.gz]
  - METEO_CSV_PATH:   percorso del CSV di output
  - METEO_CITIES_DIM: path al file cities.csv (per il mapping slug→ISTAT)

resolve_slug: prova prima mappatura diretta, poi compatto, poi fuzzy con difflib.
"""

import difflib
import glob
import gzip
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import orjson                    
import pandas as pd
from unidecode import unidecode
from utils.data_utils import clean_dataframe, pad_istat_code

# ───────────── CONFIG ─────────────
INPUT_DIR   = os.getenv("METEO_INPUT_DIR")
OUTPUT_PATH = os.getenv("METEO_CSV_PATH")
CITIES_DIM  = os.getenv("METEO_CITIES_DIM")
BATCH_SIZE  = int(os.getenv("EXTRACT_BATCH_SIZE", 200_000))

# ───────────── LOGGING ─────────────
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("extract")


def validate_env():
    for var, val in (
        ("METEO_INPUT_DIR", INPUT_DIR),
        ("METEO_CSV_PATH", OUTPUT_PATH),
        ("METEO_CITIES_DIM", CITIES_DIM),
    ):
        if not val:
            log.error("❌ Env var %s non impostata", var)
            sys.exit(1)


def make_slug(name: str) -> str:
    """Da 'San Donà di Piave' → 'san-dona-di-piave'."""
    s = unidecode(name.strip()).lower()
    for ch in ("'", ",", "."):
        s = s.replace(ch, "")
    return "-".join(s.split())


def load_city_mapping(dim_path: Path) -> dict[str, str]:
    """Costruisce mapping slug → ISTAT da cities.csv."""
    df = pd.read_csv(dim_path, sep=";", decimal=",", dtype={"codice_istat": str, "denominazione_ita": str})
    df = clean_dataframe(df, {}, ["codice_istat", "denominazione_ita"])
    df["slug"] = df["denominazione_ita"].map(make_slug)
    mapping: dict[str, str] = {}
    for slug, ist in zip(df["slug"], df["codice_istat"]):
        mapping[slug] = ist
        mapping.setdefault(slug.replace("-", ""), ist)
    return mapping


def resolve_slug(slug: str, mapping: dict[str, str]) -> str | None:
    """
    Risolve lo slug di un file JSONL in codice ISTAT:
     1. lookup diretto
     2. compatto (senza trattini)
     3. suffix‑vowel
     4. fuzzy matching
    """
    slug = unidecode(slug).lower()
    if slug in mapping:
        return mapping[slug]

    compact = slug.replace("-", "")
    if compact in mapping:
        return mapping[compact]

    for v in ("a", "e", "i", "o", "u"):
        cand = compact + v
        if cand in mapping:
            log.info("   🎯 Suffix-vowel match: '%s' + '%s' → '%s'", compact, v, cand)
            return mapping[cand]

    if len(compact) >= 5:
        candidates = difflib.get_close_matches(compact, mapping.keys(), n=1, cutoff=0.75)
        if candidates:
            log.info("   🔍 Fuzzy match: '%s' → '%s'", slug, candidates[0])
            return mapping[candidates[0]]

    return None


def parse_and_yield(path: Path, istat: str):
    """
    Legge un JSONL (o JSONL.GZ) e restituisce un generatore di dict
    già dotati di campo 'datetime' e 'istat_code'.
    """
    open_fn = gzip.open if path.suffix == ".gz" else open
    with open_fn(path, "rt", encoding="utf-8") as f:
        for line in f:
            try:
                obj = orjson.loads(line)   
            except Exception as e:
                log.error("Malformed JSON in %s: %s", path.name, e)
                continue

            records = obj if isinstance(obj, list) else [obj]
            for rec in records:
                base = {
                    "latitude": rec.get("latitude"),
                    "longitude": rec.get("longitude"),
                    "timezone": rec.get("timezone"),
                    "elevation": rec.get("elevation"),
                    "istat_code": istat,
                }
                for entry in rec.get("hourly", {}).get("data", []):
                    flat = {**entry, **base}
                    ts = flat.pop("time", None)
                    if ts is None:
                        log.error("Malformed record: missing 'time' field in %s", path.name)
                        continue
                    try:
                        flat["datetime"] = datetime.fromtimestamp(ts, timezone.utc)
                    except Exception as e:
                        log.error("Malformed timestamp in %s: %s → %s", path.name, ts, e)
                        continue
                    yield flat


def main():
    validate_env()
    input_dir  = Path(INPUT_DIR)
    output_csv = Path(OUTPUT_PATH)
    city_dim   = Path(CITIES_DIM)

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    if output_csv.exists():
        output_csv.unlink()

    city_map = load_city_mapping(city_dim)
    total    = 0
    batch    = []

    for path_str in sorted(glob.glob(str(input_dir / "*.jsonl*"))):
        p    = Path(path_str)
        slug = p.stem.replace(".jsonl", "")
        istat = resolve_slug(slug, city_map)
        if not istat:
            log.warning("⚠️ Skip %s: nessun ISTAT per slug '%s'", p.name, slug)
            continue
        log.info("Processing %s → ISTAT=%s", p.name, istat)

        for rec in parse_and_yield(p, istat):
            # Validazione row‑by‑row
            if rec.get("istat_code") is None or rec.get("datetime") is None:
                log.error("Invalid record skipped: %s", rec)
                continue

            batch.append(rec)
            if len(batch) >= BATCH_SIZE:
                try:
                    df = pd.DataFrame(batch)
                    df = clean_dataframe(df, {}, ["datetime", "istat_code"])
                    df = pad_istat_code(df, "istat_code", width=6)
                    df.to_csv(output_csv, mode="a", header=not output_csv.exists(), index=False)
                    total += len(df)
                    log.info("   ✔️  Flush %d rows (totale %d)", len(df), total)
                except Exception as e:
                    log.exception("Error writing batch to CSV: %s", e)
                batch.clear()

    # Flush finale
    if batch:
        try:
            df = pd.DataFrame(batch)
            df = clean_dataframe(df, {}, ["datetime", "istat_code"])
            df = pad_istat_code(df, "istat_code", width=6)
            df.to_csv(output_csv, mode="a", header=not output_csv.exists(), index=False)
            total += len(df)
            log.info("   ✔️  Flush finale %d rows (totale %d)", len(df), total)
        except Exception as e:
            log.exception("Error writing final batch to CSV: %s", e)

    if total == 0:
        log.error("❌ Nessun dato estratto: controlla INPUT_DIR e mapping ISTAT.")
        sys.exit(1)
    log.info("🎉 Extraction complete: %d rows in %s", total, output_csv)


if __name__ == "__main__":
    main()


