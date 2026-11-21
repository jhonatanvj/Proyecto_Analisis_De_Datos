"""Text preprocessing for the USGS notices (scraped Volcano Updates and HANS notice texts).

Observed in the real text: HTML paragraphs and <strong> labels, &nbsp; entities, URLs to other
agencies, and Hawaiian names with macrons (Kīlauea, Uēkahuna) that must survive in text_clean.

Workshop 3 changes, from the 15,829 HANS notices since 2010:
* The ʻokina is written as ʻ, ` or ' (Halemaʻumaʻu, Halema`uma`u, Pu`u `O`o). It used to become a
  space and split the names into "halema" / "uma" / "pu"; it is now removed inside words.
* Tokens fold diacritics, so "kīlauea" and "kilauea" (both spellings appear) count as one word;
  text_clean keeps the macrons for display.
* A domain stop-word list (config/stopwords_domain.txt: units, months, time zones, observatory
  acronyms...) is added to the English list.
"""
from __future__ import annotations

import re
import unicodedata
from functools import lru_cache
from pathlib import Path

from bs4 import BeautifulSoup

from ..config import ROOT

STOPWORDS_PATH = ROOT / "config" / "stopwords_en.txt"
DOMAIN_STOPWORDS_PATH = ROOT / "config" / "stopwords_domain.txt"

# keyword -> word stem, so "erupting", "eruptive" and "eruption" all count as eruption
KEYWORDS = {
    "eruption": "erupt", "ash": "ash", "lava": "lava", "seismicity": "seismic",
    "deformation": "deform", "gas": "gas", "plume": "plume",
}

_URL = re.compile(r"https?://\S+|www\.\S+")
# ʻokina and its ASCII stand-ins between or before letters (Halemaʻumaʻu, Pu`u `O`o, volcano's)
_OKINA = re.compile(r"(?<=\w)[`ʻ‘’'](?=\w)|(?<![\w`ʻ])[`ʻ](?=\w)")
_PUNCT = re.compile(r"[^\w\s]|_")
_SPACES = re.compile(r"\s+")
_TOKEN = re.compile(r"[^\W_]+")


def clean_text(html: str | None) -> str:
    if not html:
        return ""
    text = BeautifulSoup(html, "lxml").get_text(" ")
    text = unicodedata.normalize("NFKC", text).lower()
    text = _URL.sub(" ", text)
    text = _OKINA.sub("", text)
    text = _PUNCT.sub(" ", text)
    return _SPACES.sub(" ", text).strip()


@lru_cache(maxsize=1)
def _stopwords_from(path: str) -> frozenset[str]:
    lines = Path(path).read_text(encoding="utf-8").splitlines()
    return frozenset(w.strip().lower() for w in lines if w.strip() and not w.startswith("#"))


def load_stopwords(path: Path = STOPWORDS_PATH, domain: Path | None = DOMAIN_STOPWORDS_PATH) -> set[str]:
    words = set(_stopwords_from(str(path)))
    if domain is not None and domain.exists():
        words |= _stopwords_from(str(domain))
    return words


def fold(token: str) -> str:
    """kīlauea -> kilauea: drop combining marks after Unicode decomposition."""
    return "".join(c for c in unicodedata.normalize("NFKD", token) if not unicodedata.combining(c))


def tokenize(text: str, stopwords: set[str]) -> list[str]:
    tokens = (fold(t) for t in _TOKEN.findall(text or ""))
    return [t for t in tokens if len(t) > 1 and not t.isdigit() and t not in stopwords]


def keyword_flags(tokens: list[str]) -> dict[str, bool]:
    return {f"kw_{name}": any(t.startswith(stem) for t in tokens) for name, stem in KEYWORDS.items()}
