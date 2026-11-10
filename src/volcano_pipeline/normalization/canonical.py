"""Draft mappers: source record -> canonical ``event`` dict (see workshop_1/mapping.md).

These functions define the data contract agreed in Workshop #1. Workshop #2 will
add validation/quarantine around them and persist the results in PostgreSQL.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from ..sources.usgs_hans import ALERT_LEVELS
from .geo import normalize_longitude

CANONICAL_FIELDS = [
    "event_id", "source", "event_type", "timestamp", "latitude", "longitude",
    "severity", "severity_unit", "raw_reference", "content_hash",
]


def content_hash(record: Any) -> str:
    """Stable SHA-256 of a record (sorted keys) used for change detection."""
    payload = json.dumps(record, sort_keys=True, default=str, ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _ms_to_utc(ms: int | float | None) -> str | None:
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def gvp_eruption_date(year, month, day) -> tuple[str | None, str]:
    """Build an ISO date from GVP partial dates. Returns (date, precision).

    GVP stores year/month/day separately; month/day are often 0/null (unknown)
    and BCE years are negative. Missing parts default to 1 and the precision
    is reported so analysis can filter out imprecise dates.
    """
    if year is None or year == "":
        return None, "unknown"
    year = int(year)
    if year < 1:          # BCE: outside any timestamp type we use for analysis
        return None, "bce"
    m = int(month) if month not in (None, "", 0, "0") else None
    d = int(day) if day not in (None, "", 0, "0") else None
    precision = "day" if (m and d) else "month" if m else "year"
    try:
        dt = datetime(year, m or 1, d or 1, tzinfo=timezone.utc)
    except ValueError:
        return None, "invalid"
    return dt.isoformat(), precision


def map_usgs_feature(feature: dict) -> dict:
    """USGS GeoJSON feature (feed) -> canonical event."""
    p = feature["properties"]
    lon, lat, _depth = feature["geometry"]["coordinates"]
    return {
        "event_id": f"usgs:{feature['id']}",
        "source": "usgs_feed",
        "event_type": "earthquake",
        "timestamp": _ms_to_utc(p.get("time")),
        "latitude": lat,
        "longitude": normalize_longitude(lon),
        "severity": p.get("mag"),
        "severity_unit": f"magnitude ({p.get('magType')})",
        "raw_reference": p.get("url") or p.get("detail"),
        "content_hash": content_hash(feature),
    }


def map_usgs_csv_row(row: dict) -> dict:
    """USGS FDSN CSV row -> canonical event (same id space as the feed)."""
    return {
        "event_id": f"usgs:{row['id']}",
        "source": "usgs_fdsn",
        "event_type": "earthquake",
        "timestamp": row.get("time"),           # already ISO-8601 UTC with 'Z'
        "latitude": float(row["latitude"]),
        "longitude": normalize_longitude(float(row["longitude"])),
        "severity": float(row["mag"]) if row.get("mag") not in (None, "") else None,
        "severity_unit": f"magnitude ({row.get('magType')})",
        "raw_reference": f"https://earthquake.usgs.gov/earthquakes/eventpage/{row['id']}",
        "content_hash": content_hash(row),
    }


def map_gvp_eruption(props: dict) -> dict:
    """GVP eruption feature properties (from ``gvp.parse_features``) -> canonical event.

    The eruptions layer has no Latitude/Longitude properties: the position is only in
    the GeoJSON geometry, exposed by ``parse_features`` as ``_lat``/``_lon``.
    """
    ts, _precision = gvp_eruption_date(
        props.get("StartDateYear"), props.get("StartDateMonth"), props.get("StartDateDay"))
    lat = props.get("Latitude", props.get("_lat"))
    lon = props.get("Longitude", props.get("_lon"))
    return {
        "event_id": f"gvp_eruption:{props.get('Eruption_Number')}",
        "source": "gvp_eruptions",
        "event_type": "eruption",
        "timestamp": ts,
        "latitude": lat,
        "longitude": normalize_longitude(lon) if lon is not None else None,
        "severity": props.get("ExplosivityIndexMax"),
        "severity_unit": "VEI (0-8, log scale of erupted tephra volume)",
        "raw_reference": f"https://volcano.si.edu/volcano.cfm?vn={props.get('Volcano_Number')}",
        "content_hash": content_hash(props),
    }


def map_hans_notice(row: dict, volcano_coords: dict[int, tuple[float, float]]) -> dict:
    """USGS HANS monitored/elevated row -> canonical event.

    Severity is the official USGS volcano alert level as an ordinal
    (1=NORMAL, 2=ADVISORY, 3=WATCH, 4=WARNING); the aviation colour code goes to
    the ``volcano_alert`` domain table. HANS rows carry no coordinates, so the
    position comes from the volcano table (``volcano_coords[vnum] = (lat, lon)``).
    """
    vnum = int(row["vnum"])
    lat, lon = volcano_coords.get(vnum, (None, None))
    level = (row.get("alert_level") or "").upper()
    return {
        "event_id": f"usgs_hans:{vnum}:{row['notice_identifier']}",
        "source": "usgs_hans",
        "event_type": "alert_notice",
        "timestamp": _ms_to_utc(row["sent_unixtime"] * 1000) if row.get("sent_unixtime") else None,
        "latitude": lat,
        "longitude": lon,
        "severity": ALERT_LEVELS.get(level),
        "severity_unit": "USGS volcano alert level (1=NORMAL, 2=ADVISORY, 3=WATCH, 4=WARNING)",
        "raw_reference": row.get("notice_url") or row["notice_identifier"],
        "content_hash": content_hash(row),
    }


def map_weekly_item(item: dict) -> dict:
    """Parsed weekly report item (see sources/weekly_report.py) -> canonical event."""
    return {
        "event_id": f"gvp_weekly:{item['volcano_number']}:{item['week_start']}",
        "source": "gvp_weekly_rss",
        "event_type": "activity_report",
        "timestamp": item["week_end"],
        "latitude": item.get("latitude"),
        "longitude": item.get("longitude"),
        "severity": 2 if item.get("status") == "NEW" else 1,
        "severity_unit": "report status ordinal (1=ongoing, 2=new activity/unrest)",
        "raw_reference": item.get("link"),
        "content_hash": content_hash(item),
    }
