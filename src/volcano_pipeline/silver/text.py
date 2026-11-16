"""Text preprocessing for the scraped USGS notices.

Observed in the real page: HTML paragraphs and <strong> labels inside the text, &nbsp;
entities, URLs to other agencies, and Hawaiian names with macrons (Kīlauea, Uēkahuna) that
must survive. The result feeds keyword counts later; no sentiment model is applied.
"""
from __future__ import annotations

import re
import unicodedata
from functools import lru_cache
from pathlib import Path

from bs4 import BeautifulSoup

from ..config import ROOT

STOPWORDS_PATH = ROOT / "config" / "stopwords_en.txt"

# keyword -> word stem, so "erupting", "eruptive" and "eruption" all count as eruption
KEYWORDS = {
    "eruption": "erupt", "ash": "ash", "lava": "lava", "seismicity": "seismic",
    "deformation": "deform", "gas": "gas", "plume": "plume",
}

_URL = re.compile(r"https?://\S+|www\.\S+")
_PUNCT = re.compile(r"[^\w\s]|_")
_SPACES = re.compile(r"\s+")
_TOKEN = re.compile(r"[^\W_]+")


def clean_text(html: str | None) -> str:
    if not html:
        return ""
    text = BeautifulSoup(html, "lxml").get_text(" ")
    text = unicodedata.normalize("NFKC", text).lower()
    text = _URL.sub(" ", text)
    text = _PUNCT.sub(" ", text)
    return _SPACES.sub(" ", text).strip()


@lru_cache(maxsize=1)
def _stopwords_from(path: str) -> frozenset[str]:
    lines = Path(path).read_text(encoding="utf-8").splitlines()
    return frozenset(w.strip().lower() for w in lines if w.strip() and not w.startswith("#"))


def load_stopwords(path: Path = STOPWORDS_PATH) -> set[str]:
    return set(_stopwords_from(str(path)))


def tokenize(text: str, stopwords: set[str]) -> list[str]:
    return [t for t in _TOKEN.findall(text or "")
            if len(t) > 1 and not t.isdigit() and t not in stopwords]


def keyword_flags(tokens: list[str]) -> dict[str, bool]:
    return {f"kw_{name}": any(t.startswith(stem) for t in tokens) for name, stem in KEYWORDS.items()}
