"""Smithsonian / USGS Weekly Volcanic Activity Report RSS (incremental, weekly).

Format observed on 2026-11-09 (the feed is ISO-8859-1 encoded):

* title: ``Krakatau (Indonesia) - Report for 8 October-14 October 2026 - New Eruptive Activity``
  (or ``- Continuing Eruptive Activity``);
* link: the generic ``https://volcano.si.edu/reports_weekly.cfm`` page;
* guid: ``https://volcano.si.edu/reports_weekly.cfm#vn_311120`` (Smithsonian volcano number).

Older items used ``- NEW`` and links like ``showreport.cfm?wvar=GVP.WVAR20251029-211060``;
both formats are still parsed so that a format change is detected early.
"""
from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from datetime import datetime
from datetime import timezone
from email.utils import parsedate_to_datetime

MONTHS = {m: i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July", "August",
     "September", "October", "November", "December"], start=1)}

TITLE_RE = re.compile(
    r"^(?P<name>.+?)\s*\((?P<country>[^)]*)\)\s*-\s*Report for\s+(?P<range>.+?)"
    r"(?:\s*-\s*(?P<status>(?:New|Continuing|Ongoing|NEW)\b[A-Za-z /]*))?\s*$")
WVAR_RE = re.compile(r"WVAR(?P<date>\d{8})-(?P<vnum>\d{6})")
GUID_VNUM_RE = re.compile(r"vn_(?P<vnum>\d{6})")
RANGE_RE = re.compile(
    r"(?P<d1>\d{1,2})\s*(?P<m1>[A-Za-z]+)?\s*(?P<y1>\d{4})?\s*-\s*"
    r"(?P<d2>\d{1,2})\s+(?P<m2>[A-Za-z]+)\s+(?P<y2>\d{4})")
GEORSS = "{http://www.georss.org/georss}point"


def parse_week_range(text: str) -> tuple[str | None, str | None]:
    """'29 October-4 November 2025' or '31 December 2025-6 January 2026' -> ISO dates."""
    m = RANGE_RE.search(text.replace("–", "-"))
    if not m:
        return None, None
    m2 = MONTHS.get(m["m2"].capitalize())
    y2 = int(m["y2"])
    m1 = MONTHS.get((m["m1"] or m["m2"]).capitalize())
    y1 = int(m["y1"]) if m["y1"] else (y2 - 1 if m1 and m2 and m1 > m2 else y2)
    if not (m1 and m2):
        return None, None
    return (datetime(y1, m1, int(m["d1"])).date().isoformat(),
            datetime(y2, m2, int(m["d2"])).date().isoformat())


def parse_title(title: str) -> dict:
    m = TITLE_RE.match(title.strip())
    if not m:
        return {"volcano_name": None, "country": None, "week_start": None,
                "week_end": None, "status": None, "title_parse_ok": False}
    start, end = parse_week_range(m["range"])
    return {
        "volcano_name": m["name"].strip(),
        "country": m["country"].strip(),
        "week_start": start,
        "week_end": end,
        "status": "NEW" if (m["status"] or "").upper().startswith("NEW") else "ONGOING",
        "status_raw": m["status"],
        "title_parse_ok": start is not None,
    }


def parse_rss(content: bytes) -> list[dict]:
    root = ET.fromstring(content)
    items = []
    for it in root.iter("item"):
        title = (it.findtext("title") or "").strip()
        link = (it.findtext("link") or "").strip()
        guid = (it.findtext("guid") or "").strip()
        rec = {"title": title, "link": link, "guid": guid,
               "description": (it.findtext("description") or "").strip(),
               "pub_date_raw": it.findtext("pubDate"),
               **parse_title(title)}
        w = WVAR_RE.search(link)
        g = GUID_VNUM_RE.search(guid)
        rec["volcano_number"] = int(g["vnum"]) if g else int(w["vnum"]) if w else None
        if w and not rec["week_start"]:
            rec["week_start"] = datetime.strptime(w["date"], "%Y%m%d").date().isoformat()
        try:
            rec["pub_date_utc"] = parsedate_to_datetime(rec["pub_date_raw"]).astimezone(timezone.utc).isoformat()
        except (TypeError, ValueError):
            rec["pub_date_utc"] = None
        point = it.findtext(GEORSS)
        if point:
            lat, lon = point.split()[:2]
            rec["latitude"], rec["longitude"] = float(lat), float(lon)
        else:
            rec["latitude"] = rec["longitude"] = None
        items.append(rec)
    return items
