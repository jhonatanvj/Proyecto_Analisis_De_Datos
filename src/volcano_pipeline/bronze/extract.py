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

API_SOURCES = ("usgs_hans", "usgs_feed", "usgs_hans_notices", "usgs_fdsn")
GVP_SOURCES = ("gvp_volcanoes", "gvp_eruptions")
# reference catalogues downloaded once per calendar month
MONTHLY_SOURCES = GVP_SOURCES + ("usgs_hans_volcanoes",)
SCRAPE_SOURCE = "usgs_volcano_updates"


@dataclass
class Extraction:
    source_id: str
    url: str
    params: dict
    http_status: int
    content: bytes
    payload: Any


def extract(source_id: str, cfg: dict, session, bronze_dir: Path | None = None) -> Extraction:
    src = get_source(cfg, source_id)
    params: dict = {}
    if source_id == "usgs_hans_notices":
        from ..config import datalake_dir
        from .hans_notices import extract_incremental

        return extract_incremental(cfg, session, bronze_dir or datalake_dir("bronze"))
    if source_id == "usgs_fdsn":
        from ..config import datalake_dir
        from .fdsn_incremental import extract_incremental as fdsn_incremental

        return fdsn_incremental(cfg, session, bronze_dir or datalake_dir("bronze"))
    if source_id == "usgs_hans":
        resp = usgs_hans.fetch(session, src["url"], src["endpoints"]["monitored"])
        payload = json.loads(resp.content)
    elif source_id == "usgs_hans_volcanoes":
        resp = usgs_hans.fetch(session, src["url"], src["endpoints"]["us_volcanoes"])
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


def after_persist(source_id: str, payload, directory: Path, when: datetime) -> None:
    """Source state that moves only once the Bronze file is safely written."""
    if source_id in MONTHLY_SOURCES:
        mark_checked(directory, source_id, when)
    elif source_id == "usgs_hans_notices":
        from .hans_notices import advance_watermark

        advance_watermark(directory, payload or [])
    elif source_id == "usgs_fdsn":
        from .fdsn_incremental import advance_watermark as fdsn_advance

        fdsn_advance(directory, payload or {})


def _marker(directory: Path, source_id: str) -> Path:
    return directory / "_state" / f"{source_id}.last_checked"


def mark_checked(directory: Path, source_id: str, when: datetime) -> None:
    """Remember the month a source was downloaded, also when its content was unchanged
    (write_bronze then writes no file, so the file names alone cannot tell)."""
    path = _marker(directory, source_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"{when:%Y%m}", encoding="utf-8")


def gvp_due(directory: Path, source_id: str, now: datetime) -> bool:
    """GVP is a static catalogue: download it once per calendar month."""
    month = f"{now:%Y%m}"
    marker = _marker(directory, source_id)
    if marker.exists() and marker.read_text(encoding="utf-8").strip() == month:
        return False
    return not any(p.name.startswith(f"{source_id}_{month}") for p in bronze_files(directory, source_id))
