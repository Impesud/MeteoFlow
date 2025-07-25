import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Union

import pandas as pd
from sqlalchemy.engine import Engine

logger = logging.getLogger("data_utils")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")


def read_jsonl(path: Union[str, Path]) -> pd.DataFrame:
    """
    Legge un file JSONL e lo converte in un DataFrame pandas.
    Se le righe contengono liste di oggetti, le "appiattisce" in righe separate.

    Args:
        path: Percorso al file .jsonl

    Returns:
        pd.DataFrame con tutte le proprietà JSON "appiattite".
    """
    records = []
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"read_jsonl: file non trovato: {path}")
    with path.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            obj = json.loads(line)
            if isinstance(obj, list):
                records.extend(obj)
            else:
                records.append(obj)
    # usa json_normalize per eventuali campi annidati
    df = pd.json_normalize(records)
    logger.info("read_jsonl: letto %d record da %s", len(df), path)
    return df


def clean_dataframe(
    df: pd.DataFrame, rename_map: Optional[Dict[str, str]] = None, required_cols: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Pulisce un DataFrame:
      - Rinomina colonne secondo `rename_map`
      - Rimuove duplicati
      - Elimina righe con valori nulli in `required_cols`

    Args:
        df: DataFrame di input
        rename_map: mappa {vecchio_nome: nuovo_nome}
        required_cols: lista di colonne che non devono contenere NaN

    Returns:
        pd.DataFrame pulito
    """
    df = df.copy()
    if rename_map:
        df = df.rename(columns=rename_map)
        logger.debug("clean_dataframe: rinominate colonne %s", rename_map)
    before = len(df)
    df = df.drop_duplicates()
    if required_cols:
        df = df.dropna(subset=required_cols)
        logger.debug("clean_dataframe: tolte righe con NA in %s", required_cols)
    after = len(df)
    logger.info("clean_dataframe: %d → %d righe dopo pulizia", before, after)
    return df


def pad_istat_code(df: pd.DataFrame, col: str, width: int = 6) -> pd.DataFrame:
    """
    Aggiunge zeri a sinistra a una colonna di codici ISTAT.

    Args:
        df: DataFrame di input
        col: nome della colonna da riempire
        width: lunghezza finale desiderata (default 6)

    Returns:
        pd.DataFrame con la colonna modificata
    """
    if col not in df.columns:
        logger.error("pad_istat_code: colonna '%s' non trovata", col)
        raise KeyError(f"Colonna '{col}' non presente nel DataFrame")
    df = df.copy()
    df[col] = df[col].astype(str).str.zfill(width)
    logger.info("pad_istat_code: colonna '%s' zfilled a width=%d", col, width)
    return df


def load_dataframe(
    df: pd.DataFrame, table: str, engine: Engine, if_exists: str = "append", chunksize: Optional[int] = None
) -> None:
    """
    Carica un DataFrame in una tabella Postgres tramite SQLAlchemy.

    Args:
        df: DataFrame da caricare
        table: nome della tabella di destinazione
        engine: SQLAlchemy Engine già configurato
        if_exists: comportamento se la tabella esiste ("fail", "replace", "append")
        chunksize: numero di righe per batch (None = un unico batch)

    Raises:
        ValueError se il DataFrame è vuoto
        Exception su errori di caricamento
    """
    if df.empty:
        logger.warning("load_dataframe: DataFrame vuoto, skip caricamento su '%s'", table)
        return

    try:
        logger.info("load_dataframe: inizio caricamento %d righe in '%s'", len(df), table)
        df.to_sql(
            name=table,
            con=engine,
            if_exists=if_exists,
            index=False,
            method="multi",
            chunksize=chunksize,
        )
        logger.info("load_dataframe: caricati %d righe in '%s'", len(df), table)
    except Exception:
        logger.exception("load_dataframe: errore caricamento in '%s'", table)
        raise
