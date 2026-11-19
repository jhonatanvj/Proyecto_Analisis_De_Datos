"""Basic schema check of each source payload before it is written to Bronze.

The checks are deliberately shallow: they catch an API that changed shape or a scraped page
whose layout changed (no blocks found), not individual bad records, which Silver quarantines.
"""
from __future__ import annotations

from typing import Any

HANS_KEYS = {"vnum", "alert_level", "notice_identifier"}
GEOJSON_SOURCES = {"usgs_feed", "usgs_fdsn", "gvp_volcanoes", "gvp_eruptions"}
SCRAPE_TYPES = {"elevated", "observatory_notice"}
NOTICE_KEYS = {"noticeIdentifier", "sentUnixtime", "noticeHtml"}
HANS_VOLCANO_KEYS = {"volcano_cd", "vnum"}


def count_records(source_id: str, payload: Any) -> int:
    if isinstance(payload, dict):
        return len(payload.get("features") or [])
    if isinstance(payload, list):
        return len(payload)
    return 0


def validate_payload(source_id: str, payload: Any) -> list[str]:
    errors: list[str] = []
    if source_id == "usgs_hans":
        if not isinstance(payload, list) or not payload:
            return ["expected a non-empty list of volcano rows"]
        missing = HANS_KEYS - set(payload[0]) if isinstance(payload[0], dict) else HANS_KEYS
        if missing:
            errors.append(f"rows miss keys {sorted(missing)}")
    elif source_id in GEOJSON_SOURCES:
        if not isinstance(payload, dict) or not isinstance(payload.get("features"), list):
            return ["expected a GeoJSON FeatureCollection with a 'features' list"]
        if payload["features"] and "properties" not in payload["features"][0]:
            errors.append("features have no 'properties'")
    elif source_id == "usgs_hans_notices":
        if not isinstance(payload, list):    # an empty list is fine: no notice in the window
            return ["expected a list of notices"]
        if payload and (not isinstance(payload[0], dict) or NOTICE_KEYS - set(payload[0])):
            errors.append(f"notices miss keys {sorted(NOTICE_KEYS - set(payload[0] or {}))}")
    elif source_id == "usgs_hans_volcanoes":
        if not isinstance(payload, list) or not payload:
            return ["expected a non-empty list of volcanoes"]
        if not isinstance(payload[0], dict) or HANS_VOLCANO_KEYS - set(payload[0]):
            errors.append("volcano rows miss volcano_cd / vnum")
    elif source_id == "usgs_volcano_updates":
        if not isinstance(payload, list) or not any(
                isinstance(r, dict) and r.get("record_type") in SCRAPE_TYPES for r in payload):
            return ["no volcano blocks found in the page (layout changed?)"]
    else:
        errors.append(f"unknown source_id '{source_id}'")
    return errors
