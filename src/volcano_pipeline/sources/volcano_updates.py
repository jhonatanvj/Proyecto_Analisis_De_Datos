"""Web scraping of the USGS Volcano Updates page (https://www.usgs.gov/programs/VHP/volcano-updates).

The page is server-rendered HTML (robots.txt allows /programs/) and holds two kinds of blocks:

* ``div.usgs-vol-up-elevated`` inside ``#elevated``: one block per volcano above NORMAL/GREEN,
  with alert level, colour code, an "As of ... UTC" time, a one-line synopsis and the HANS notice URL.
* ``div.usgs-tabs-main`` with an observatory id (avo, calvo, cvo, hvo, nmi, yvo): the latest
  notice or update of that observatory, with the full activity text.

Formats differ between observatories (HVO daily updates have no notice number, CVO uses an
empty ``VNUM #``), so every field is optional except the record type and the text.
"""
from __future__ import annotations

import re

import requests
from bs4 import BeautifulSoup

from .http import get

SKIP_TABS = {"elevated", "vonas"}


def fetch(session: requests.Session, url: str) -> requests.Response:
    return get(session, url)


def _search(pattern: str, text: str) -> str | None:
    m = re.search(pattern, text)
    return m.group(1).strip() if m else None


def _parse_elevated(block) -> dict:
    bolds = block.find_all("b")
    text = block.get_text(" ", strip=True)
    synopsis = bolds[3] if len(bolds) > 3 else None
    link = block.find("a")
    observatory = None
    if synopsis is not None:
        first_word = synopsis.get_text(" ", strip=True).split(" ", 1)[0]
        observatory = first_word.lower() if first_word.isupper() else None
    return {
        "record_type": "elevated",
        "observatory": observatory,
        "volcano_name": bolds[0].get_text(strip=True) if bolds else None,
        "vnum": None,
        "alert_level": bolds[1].get_text(strip=True) if len(bolds) > 1 else None,
        "color_code": bolds[2].get_text(strip=True) if len(bolds) > 2 else None,
        "as_of_raw": _search(r"As of (.+? UTC)", text),
        "issued_raw": None,
        "notice_number": None,
        "notice_url": link.get("href") if link else None,
        "text_html": synopsis.decode_contents() if synopsis is not None else "",
    }


def _parse_observatory(tab) -> dict:
    text = tab.get_text(" ", strip=True)
    name = _search(r"Volcano:\s*(.+?)\s*\(VNUM", text) or _search(r"([A-Z][A-Z' .\-]+?)\s*\(VNUM #", text)
    spans = [tab.find("span", attrs={"name": n}) for n in ("synopsis", "summary")]
    return {
        "record_type": "observatory_notice",
        "observatory": tab.get("id"),
        "volcano_name": name,
        "vnum": _search(r"VNUM #(\d{6})", text),
        "alert_level": _search(r"Current Volcano Alert Level:\s*(?:all\s+)?([A-Z]+)", text),
        "color_code": _search(r"Current Aviation Color Code:\s*(?:all\s+)?([A-Z]+)", text),
        "as_of_raw": None,
        "issued_raw": _search(r"\(([^()]*\d{1,2}:\d{2} UTC)\)", text) or _search(r"Issued:\s*(.+?\d{1,2}:\d{2} [AP]M \w+)", text),
        "notice_number": _search(r"Notice Number:\s*(\S+)", text),
        "notice_url": None,
        "text_html": "".join(str(s) for s in spans if s is not None),
    }


def parse_volcano_updates(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    records = []
    elevated = soup.find(id="elevated")
    if elevated is not None:
        records += [_parse_elevated(b) for b in elevated.find_all("div", class_="usgs-vol-up-elevated")]
    for tab in soup.find_all("div", class_="usgs-tabs-main"):
        if tab.get("id") not in SKIP_TABS:
            records.append(_parse_observatory(tab))
    return records
