import pandas as pd

from src.scripts.utils.data_utils import clean_dataframe, pad_istat_code, read_jsonl


def test_clean_and_pad(tmp_path):
    df = pd.DataFrame(
        [
            {"a": 1, "b": 2},
            {"a": 1, "b": None},
            {"a": 2, "b": 3},
        ]
    )
    cleaned = clean_dataframe(df, rename_map={}, required_cols=["a", "b"])
    # solo righe con b non-null
    assert len(cleaned) == 2

    # test pad ISTAT
    df2 = pd.DataFrame([{"istat": "12"}, {"istat": "1234"}])
    padded = pad_istat_code(df2, "istat", width=6)
    assert padded["istat"].tolist() == ["000012", "001234"]


def test_read_jsonl(tmp_path):
    p = tmp_path / "x.jsonl"
    p.write_text('{"foo":1}\n{"foo":2}\n')
    df = read_jsonl(str(p))
    assert list(df["foo"]) == [1, 2]
