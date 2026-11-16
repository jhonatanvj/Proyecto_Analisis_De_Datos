"""Fetch one source and return its bytes plus the payload that goes into the Bronze envelope."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from ..config import get_source
from ..sources import gvp, usgs_feed, usgs_hans, volcano_updates
from .envelope import bronze_files

API_SOURCES = ("usgs_hans", "usgs_feed")
GVP_SOURCES = ("gvp_volcanoes", "gvp_eruptions")
SCRAPE_SOURCE = "usgs_volcano_updates"


@dataclass
class Extraction:
    source_id: str
    url: str
    params: dict
    http_status: int
    content: bytes
    payload: Any


def extract(source_id: str, cfg: dict, session) -> Extraction:
    src = get_source(cfg, source_id)
    params: dict = {}
    if source_id == "usgs_hans":
        resp = usgs_hans.fetch(session, src["url"], src["endpoints"]["monitored"])
        payload = json.loads(resp.content)
    elif source_id == "usgs_feed":
        resp = usgs_feed.fetch(session, src["url"])
        payload = json.loads(resp.content)
    elif source_id in GVP_SOURCES:
        params = dict(src["params"])
        resp = gvp.fetch_layer(session, src)
        payload = json.loads(resp.content)
    elif source_id == SCRAPE_SOURCE:
        resp = volcano_updates.fetch(session, src["url"])
        payload = volcano_updates.parse_volcano_updates(resp.content.decode("utf-8", errors="replace"))
    else:
        raise KeyError(f"No extractor for source '{source_id}'")
    return Extraction(source_id, str(resp.url), params, resp.status_code, resp.content, payload)


def gvp_due(directory: Path, source_id: str, now: datetime) -> bool:
    """GVP is a static catalogue: download it once per calendar month."""
    month = f"{source_id}_{now:%Y%m}"
    return not any(p.name.startswith(month) for p in bronze_files(directory, source_id))
