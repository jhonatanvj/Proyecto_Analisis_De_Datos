"""Incremental FDSN earthquakes: events created or revised since the watermark.

The backfill (fdsn_backfill.py) loads 2010 -> today once with radial queries per volcano. After
that, each run asks FDSN for the events whose ``updated`` time is after the watermark
(``updatedafter``), so a magnitude or location revised by the network is picked up too, not only
new events. One query per US volcanic region (config usgs_fdsn.incremental.regions) is enough;
Silver keeps only the events within the volcano radius.

Watermark: the newest ``properties.updated`` already written to Bronze, stored in
``datalake_bronze/_state/usgs_fdsn.watermark`` and advanced only after the Bronze file was
written. Each run starts ``overlap_minutes`` before it: an event read twice has the same id and
content hash and Silver drops the copy.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from ..config import get_source
from ..sources.http import get
from .extract import Extraction

SOURCE_ID = "usgs_fdsn"


def _state_file(bronze_dir: Path) -> Path:
    return bronze_dir / "_state" / f"{SOURCE_ID}.watermark"


def read_watermark(bronze_dir: Path) -> datetime | None:
    path = _state_file(bronze_dir)
    try:
        return datetime.fromisoformat(path.read_text(encoding="utf-8").strip()) if path.exists() else None
    except ValueError:
        return None


def newest_update(payload: dict) -> datetime | None:
    stamps = [f.get("properties", {}).get("updated") for f in (payload or {}).get("features", [])]
    stamps = [s for s in stamps if isinstance(s, (int, float))]
    return datetime.fromtimestamp(max(stamps) / 1000, tz=timezone.utc) if stamps else None


def advance_watermark(bronze_dir: Path, payload: dict) -> datetime | None:
    newest, current = newest_update(payload), read_watermark(bronze_dir)
    if newest is None or (current is not None and newest <= current):
        return current
    path = _state_file(bronze_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    # backfill tasks run in parallel: a private temp name per process. A race can at worst leave a
    # slightly older watermark, which only makes the next run re-read a few events.
    tmp = path.with_suffix(f".{os.getpid()}.part")
    tmp.write_text(newest.isoformat(), encoding="utf-8")
    tmp.replace(path)
    return newest


def updated_after(cfg: dict, bronze_dir: Path, now: datetime) -> datetime:
    inc = get_source(cfg, SOURCE_ID)["incremental"]
    mark = read_watermark(bronze_dir)
    if mark is None:
        return now - timedelta(days=int(inc.get("first_window_days", 7)))
    return mark - timedelta(minutes=float(inc.get("overlap_minutes", 60)))


def region_params(cfg: dict, box: list[float], since: datetime, now: datetime) -> dict:
    inc = get_source(cfg, SOURCE_ID)["incremental"]
    min_lat, max_lat, min_lon, max_lon = box
    fmt = "%Y-%m-%dT%H:%M:%S"
    return {
        "format": "geojson", "orderby": "time-asc", "limit": int(inc.get("max_events", 20000)),
        "updatedafter": since.astimezone(timezone.utc).strftime(fmt),
        "starttime": inc.get("event_start", "2010-01-01"), "endtime": now.astimezone(timezone.utc).strftime(fmt),
        "minmagnitude": cfg["spatial"]["min_magnitude"],
        "minlatitude": min_lat, "maxlatitude": max_lat, "minlongitude": min_lon, "maxlongitude": max_lon,
    }


def extract_incremental(cfg: dict, session, bronze_dir: Path, now: datetime | None = None) -> Extraction:
    """One FeatureCollection with the events of every region (an event id appears once)."""
    now = now or datetime.now(timezone.utc)
    src, inc = get_source(cfg, SOURCE_ID), get_source(cfg, SOURCE_ID)["incremental"]
    since = updated_after(cfg, bronze_dir, now)
    features: dict[str, dict] = {}
    raw, status = b"", 200
    for name, box in inc["regions"].items():
        params = region_params(cfg, box, since, now)
        resp = get(session, src["url"], params=params)
        data = json.loads(resp.content)
        if len(data.get("features", [])) >= params["limit"]:
            raise RuntimeError(f"FDSN returned the maximum of {params['limit']} events for {name}: "
                               "the window is too wide, run the backfill for that period instead")
        for feat in data.get("features", []):
            features[feat["id"]] = feat
        raw += resp.content
        status = resp.status_code
    payload = {"type": "FeatureCollection", "metadata": {"updatedafter": since.isoformat(),
               "regions": list(inc["regions"])}, "features": list(features.values())}
    params = {"updatedafter": since.isoformat(), "endtime": now.isoformat(), "regions": list(inc["regions"])}
    return Extraction(SOURCE_ID, src["url"], params, status, raw, payload)
