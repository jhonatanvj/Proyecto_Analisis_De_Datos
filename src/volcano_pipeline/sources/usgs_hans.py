"""USGS HANS public API: official alert levels and aviation colour codes of US volcanoes.

Endpoints verified on 2026-11-09 (JSON, no key, base ``/hans-public/api``):

* ``volcano/getUSVolcanoes``        -> list of ~170 US volcanoes (vnum, name, lat/lon, observatory,
  ``nvews_threat``). Reference data used to locate notices.
* ``volcano/getMonitoredVolcanoes`` -> latest notice per monitored volcano (~69 rows):
  ``vnum``, ``alert_level``, ``color_code``, ``notice_identifier``, ``sent_utc``, ``sent_unixtime``.
  This is the incremental source: a new ``(vnum, notice_identifier)`` pair is a new notice.
* ``volcano/getElevatedVolcanoes``  -> same fields, only volcanoes above NORMAL/GREEN.

Known quirks: ``vnum`` is a string and some rows use placeholders (``"0"``, ``"999999"``);
``sent_utc`` has no time-zone suffix (it is UTC), ``sent_unixtime`` is epoch seconds.
"""
from __future__ import annotations

import json

import requests

from .http import get

ALERT_LEVELS = {"NORMAL": 1, "ADVISORY": 2, "WATCH": 3, "WARNING": 4}
COLOR_CODES = {"GREEN": 1, "YELLOW": 2, "ORANGE": 3, "RED": 4}
PLACEHOLDER_VNUMS = {"", "0", "999999"}


def fetch(session: requests.Session, base_url: str, endpoint: str) -> requests.Response:
    return get(session, f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}")


def parse(content: bytes) -> list[dict]:
    data = json.loads(content)
    return data if isinstance(data, list) else [data]


def valid_vnum(vnum) -> bool:
    """GVP volcano numbers are 6-digit integers; HANS also emits placeholders."""
    s = str(vnum or "").strip()
    return s not in PLACEHOLDER_VNUMS and s.isdigit() and len(s) == 6


def reject_reason(row: dict) -> str | None:
    """Validation of a monitored/elevated row before it becomes an ``alert_notice`` event.

    Observed on 2026-11-09: region-wide notices ("Alaskan Volcanoes", "Cascade Range")
    have ``vnum = null``; volcanoes that never had a notice come with
    ``notice_identifier = null`` and ``sent_unixtime = 0`` (their level is the default
    NORMAL/GREEN, kept in ``volcano_alert`` as a state without an event).
    """
    if row.get("vnum") in (None, ""):
        return "region_level_notice"
    if not valid_vnum(row["vnum"]):
        return "invalid_vnum"
    if not row.get("notice_identifier") or not row.get("sent_unixtime"):
        return "no_notice_issued"
    if (row.get("alert_level") or "").upper() not in ALERT_LEVELS:
        return "unknown_alert_level"
    return None


def new_notices(rows: list[dict], seen: set[tuple[str, str]]) -> list[dict]:
    """Rows whose (vnum, notice_identifier) was not committed before."""
    return [r for r in rows if (str(r.get("vnum")), r.get("notice_identifier")) not in seen]
