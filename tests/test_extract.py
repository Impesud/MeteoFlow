import logging

import pytest

from src.scripts.extract import resolve_slug


# ❷ Always capture INFO‐level logs
@pytest.fixture(autouse=True)
def set_log_level(caplog):
    caplog.set_level(logging.INFO)


def test_direct_lookup():
    mapping = {"roma": "058091", "milano": "015146"}
    # case‐insensitive via unidecode.lower()
    assert resolve_slug("roma", mapping) == "058091"
    assert resolve_slug("Milano", mapping) == "015146"


def test_compact_lookup():
    mapping = {"sanremo": "008056"}
    assert resolve_slug("san-remo", mapping) == "008056"


@pytest.mark.parametrize(
    "vowel,expected",
    [
        ("a", "001001"),
        ("e", "001002"),
        ("i", "001003"),
        ("o", "001004"),
        ("u", "001005"),
    ],
)
def test_suffix_vowel_lookup(vowel, expected, caplog):
    slug = "testslug"  # compact == "testslug"
    mapping = {slug + vowel: expected}
    caplog.clear()
    res = resolve_slug("testslug", mapping)
    assert res == expected
    assert "Suffix-vowel match" in caplog.text


def test_no_match_short_slug(caplog):
    mapping = {"abc": "000"}
    res = resolve_slug("xyz", mapping)
    assert res is None
    # neither suffix nor fuzzy for a short slug without mapping
    assert "Suffix-vowel match" not in caplog.text
    assert "Fuzzy match" not in caplog.text
