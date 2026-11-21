"""USGS HANS notice search: full text of every volcano notice since 2008 (Workshop 3 text corpus).

Endpoint (verified 2026-11-19): ``POST {base}/search/search`` with a JSON body
``{"obsAbbr": "", "noticeTypeCd": "", "volcCd": "", "startUnixtime": <s>, "endUnixtime": <s>,
"searchText": "", "pageIndex": <n>}`` returns ``{"noticeTotal", "noticeData", "searchOptions"}``.
``noticeData`` holds up to 20 notices, newest first, each with ``noticeIdentifier``, ``sentUtc``,
``sentUnixtime``, ``noticeTypeCd``, ``obsAbbr``, ``volcCds`` (USGS volcano codes, comma
separated, empty for region-wide notices), ``permLink`` and the full ``noticeHtml``.
``search/preflight`` takes the same body and returns only ``noticeTotal``.

Observed quirks: ``obsAbbr: "all"`` matches nothing (the documented value), an empty string
matches every observatory. About 16,500 notices exist since 2010 (2026-11-19), 670-1,100 a year;
one page is ~170 kB and takes ~1.4 s.

Notice types: DU daily update, WU weekly update, SR status report, VAN volcanic activity notice,
IS information statement, VV / VONA notice for aviation, MU monthly update, BU biweekly update.
"""
from __future__ import annotations

import re
import time
from datetime import datetime, timezone
from typing import Iterator

from bs4 import BeautifulSoup

PAGE_SIZE = 20
MAX_PAGES = 2000                    # safety stop: 40,000 notices, more than the whole archive
ALERT_LEVELS = {"NORMAL": 1, "ADVISORY": 2, "WATCH": 3, "WARNING": 4}
COLOR_CODES = {"GREEN": 1, "YELLOW": 2, "ORANGE": 3, "RED": 4}


def search_body(start: datetime, end: datetime | None, page: int | None = None) -> dict:
    body = {"obsAbbr": "", "noticeTypeCd": "", "volcCd": "", "searchText": "",
            "startUnixtime": int(start.timestamp()),
            "endUnixtime": int(end.timestamp()) if end else ""}
    if page is not None:
        body["pageIndex"] = page
    return body


def search_url(base_url: str) -> str:
    return f"{base_url.rstrip('/')}/search/search"


def search_page(session, base_url: str, start: datetime, end: datetime | None, page: int,
                attempts: int = 4, backoff_s: float = 5.0):
    """One search page. The server sometimes answers 200 with an empty or non-JSON body (seen on
    2026-11-19 in the middle of the backfill; the same request worked a minute later). HTTP-level
    retries do not catch that, so the body is checked and the request repeated with a growing wait."""
    for attempt in range(1, attempts + 1):
        resp = session.post(search_url(base_url), json=search_body(start, end, page), timeout=(10, 120))
        resp.raise_for_status()
        try:
            data = resp.json()
            if isinstance(data, dict) and "noticeData" in data:
                return resp, data
        except ValueError:
            pass
        if attempt == attempts:
            raise RuntimeError(f"HANS search page {page} ({start:%Y-%m-%d}) returned no JSON after {attempts} tries")
        time.sleep(backoff_s * 2 ** (attempt - 1))


def search_window(session, base_url: str, start: datetime, end: datetime | None,
                  pause_s: float = 1.0, backoff_s: float = 5.0) -> Iterator[tuple[int, object, dict]]:
    """Yield (page index, response, parsed JSON) for every page of notices sent in [start, end)."""
    for page in range(MAX_PAGES):
        resp, data = search_page(session, base_url, start, end, page, backoff_s=backoff_s)
        yield page, resp, data
        if len(data.get("noticeData") or []) < PAGE_SIZE:
            return
        time.sleep(pause_s)        # polite: one request per second
    raise RuntimeError(f"more than {MAX_PAGES} pages between {start} and {end}")


def fetch_window(session, base_url: str, start: datetime, end: datetime | None,
                 pause_s: float = 1.0, backoff_s: float = 5.0) -> tuple[list[dict], bytes, int]:
    """All notices of a window as received, plus the raw bytes (for the Bronze SHA-256)."""
    notices, raw, status = [], b"", 200
    for _, resp, data in search_window(session, base_url, start, end, pause_s, backoff_s):
        notices += data.get("noticeData") or []
        raw += resp.content
        status = resp.status_code
    return notices, raw, status


# --------------------------------------------------------------------------- notice text

_SECTION = re.compile(r"^\s*([A-Z][A-Za-z0-9 /&-]{2,45}):\s*(.*)$")
# labels of the observatory's own assessment, newest format first; older notices (2010-2019) use
# "Activity Summary for past 24 hours", "Volcanic Activity Summary", "Activity Update", "Remarks"
_SUMMARY_LABELS = ("summary", "rmk", "remarks", "activity summary", "volcanic activity summary", "activity update")
# fixed paragraphs repeated in hundreds of notices: they would dominate the keyword counts
_DROP_SECTIONS = ("background", "definitions of terms used", "additional information")
# header lines: title, agency, date, position, elevation, alert state, notice metadata
_HEADER = re.compile(
    r"^\s*(\(?vnum\b|current (volcano alert level|aviation colou?r code)"
    r"|previous (volcano alert level|aviation colou?r code)"
    r"|summit elevation|elevation:|location:|notice number:|issued:|source:|area:|volcano:"
    r"|u\.s\. geological survey$|.*volcano observatory (daily|weekly|monthly|biweekly) update$"
    r"|.*volcano observatory status report$|\w+day, \w+ \d{1,2}, \d{4}, \d{1,2}:\d{2}"
    r"|\d{1,3}°\s*\d{1,2}'|n\s?\d{1,2} deg)", re.IGNORECASE)
# Footer blocks repeated in every notice of an observatory: contact data, links, mission text
_FOOTER = re.compile(
    r"^\s*(contact information|contacts?|subscribe to these messages|for more information|more information"
    r"|(hvo|avo|cvo|calvo|yvo) contact information"
    r"|the (hawaiian|alaska|cascades|california|yellowstone) volcano observatory is"
    r"|as part of the u\.s\. geological survey's volcano hazards program"
    r"|nnnn\s*$)", re.IGNORECASE)
_URL_LINE = re.compile(r"^\s*(\S.{0,80}:\s*)?(https?://\S+|www\.\S+)\s*[.,]?\s*$")
_LEVEL = re.compile(r"volcano alert level:\s*(?:all\s+)?(normal|advisory|watch|warning)", re.IGNORECASE)
_COLOR = re.compile(r"(?:aviation colou?r code|current colou?r code):\s*(?:all\s+)?(green|yellow|orange|red)",
                    re.IGNORECASE)
_VNUM = re.compile(r"(?:VNUM\s*#?\s*|VOLCANO:\s*[A-Z' .-]+\s)(\d{6})")


def html_lines(html: str | None) -> list[str]:
    if not html:
        return []
    text = BeautifulSoup(html, "lxml").get_text("\n")
    return [ln.strip() for ln in text.splitlines() if ln.strip()]


def strip_footer(lines: list[str]) -> list[str]:
    """Drop everything from the first footer marker on, and lines that are only a link."""
    out = []
    for ln in lines:
        if _FOOTER.match(ln):
            break
        if not _URL_LINE.match(ln):
            out.append(ln)
    return out


def _label(line: str) -> str | None:
    m = _SECTION.match(line)
    return m.group(1).strip().lower() if m else None


def narrative(lines: list[str]) -> list[str]:
    """Body without header lines and without the fixed Background / Definitions sections."""
    out, skipping = [], False
    for ln in lines:
        label = _label(ln)
        if label is not None:
            skipping = label.startswith(_DROP_SECTIONS)
        if skipping or _HEADER.match(ln):
            continue
        out.append(ln)
    return out


def summary_text(lines: list[str]) -> str:
    """The observatory's own assessment: the 'Summary:' section (RMK: in a VONA, 'Activity Summary'
    or 'Activity Update' in older notices). Without such a label, the first narrative paragraph of at
    least 80 characters stands in for it."""
    for i, ln in enumerate(lines):
        label = _label(ln)
        if label and label.startswith(_SUMMARY_LABELS):
            rest = _SECTION.match(ln).group(2)
            parts = [rest] if rest else []
            for nxt in lines[i + 1:]:
                if _SECTION.match(nxt):
                    break
                parts.append(nxt)
            text = " ".join(p for p in parts if p).strip()
            if text:
                return text
    return next((ln for ln in narrative(lines) if len(ln) >= 80 and not _label(ln)), "")


def highest(pattern: re.Pattern, text: str, scale: dict[str, int]) -> str | None:
    found = {m.group(1).upper() for m in pattern.finditer(text)}
    return max(found, key=scale.get) if found else None


def parse_notice(html: str | None) -> dict:
    """Plain text pieces and the alert state written in a notice."""
    lines = html_lines(html)
    body = strip_footer(lines)
    full = "\n".join(lines)
    return {
        "summary_raw": summary_text(body),
        "body_raw": " ".join(narrative(body)),
        "alert_level": highest(_LEVEL, full, ALERT_LEVELS),
        "color_code": highest(_COLOR, full, COLOR_CODES),
        "vnums_in_text": sorted({int(v) for v in _VNUM.findall(full)}),
    }


def sent_time(row: dict) -> datetime | None:
    try:
        return datetime.fromtimestamp(int(row["sentUnixtime"]), tz=timezone.utc)
    except (KeyError, TypeError, ValueError):
        return None
