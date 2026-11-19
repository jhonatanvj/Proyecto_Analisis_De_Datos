"""One-off historical load of earthquakes around US volcanoes from the USGS FDSN event service.

For each volcano: ask the /count endpoint how many events (M >= min_magnitude, within the
configured radius) exist between start and end. The service returns at most 20,000 events per
request, so busy volcanoes (Kilauea, Clear Lake...) are split into halves of the date range
until every part fits. Each part is then downloaded as GeoJSON (same format and ids as the
real-time feed) and stored as a normal Bronze file, so the Silver DAG processes it unchanged.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

from ..config import get_source
from ..sources.http import get
from ..storage.raw_landing import landing_name
from .extract import Extraction
from .fdsn_incremental import advance_watermark

log = logging.getLogger(__name__)

SOURCE_ID = "usgs_fdsn"
MAX_EVENTS = 20_000
MIN_SPAN = timedelta(days=1)


BLANK_DATES = {"", "empty", "today", "none"}


def parse_day(value: str | None, default) -> datetime:
    """Date parameter typed by a person in the Airflow form: blank (or 'empty', 'today') means default."""
    from datetime import date

    text = (value or "").strip()
    if text.lower() in BLANK_DATES:
        d = default
    else:
        try:
            d = date.fromisoformat(text)
        except ValueError:
            raise ValueError(f"invalid date {value!r}: use YYYY-MM-DD or leave it blank") from None
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


def volcano_coords_from_bronze(bronze_dir: Path, only: list[int] | None = None) -> dict[int, tuple[float, float]]:
    """Positions of the US volcanoes from the newest gvp_volcanoes Bronze file (empty if there is none)."""
    from ..silver.transform import build_context
    from .envelope import latest_bronze

    path = latest_bronze(bronze_dir, "gvp_volcanoes")
    if path is None:
        return {}
    coords = build_context(json.loads(path.read_text(encoding="utf-8")))["volcano_coords"]
    return {v: c for v, c in coords.items() if not only or v in set(only)}


def plan_partitions(count: Callable[[datetime, datetime], int], start: datetime, end: datetime,
                    limit: int = MAX_EVENTS) -> list[tuple[datetime, datetime]]:
    """Date ranges that each hold at most `limit` events (or are one day long). Empty ranges are dropped."""
    n = count(start, end)
    if n == 0:
        return []
    if n <= limit or end - start <= MIN_SPAN:
        if n > limit:
            log.warning("%d events in one day (%s); USGS will truncate to %d", n, start, limit)
        return [(start, end)]
    mid = start + (end - start) / 2
    return plan_partitions(count, start, mid, limit) + plan_partitions(count, mid, end, limit)


def _query_params(cfg: dict, lat: float, lon: float, start: datetime, end: datetime) -> dict:
    spatial = cfg["spatial"]
    return {
        "latitude": lat, "longitude": lon, "maxradiuskm": spatial["volcano_radius_km"],
        "minmagnitude": spatial["min_magnitude"],
        "starttime": start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
        "endtime": end.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
    }


def volcano_partitions(volcano_coords: dict[int, tuple[float, float]], cfg: dict, session,
                       start: datetime, end: datetime) -> list[dict]:
    src = get_source(cfg, SOURCE_ID)
    count_url = src["url"].replace("/query", "/count")
    parts = []
    for vnum, (lat, lon) in sorted(volcano_coords.items()):
        def count(a: datetime, b: datetime, lat=lat, lon=lon) -> int:
            resp = get(session, count_url, params={**_query_params(cfg, lat, lon, a, b), "format": "text"})
            return int(resp.text.strip())
        for a, b in plan_partitions(count, start, end):
            parts.append({"vnum": vnum, "lat": lat, "lon": lon, "start": a.isoformat(),
                          "end": b.isoformat(), "expected": count(a, b)})
    return parts


def fetch_partition(part: dict, cfg: dict, session) -> Extraction:
    src = get_source(cfg, SOURCE_ID)
    params = {**_query_params(cfg, part["lat"], part["lon"], datetime.fromisoformat(part["start"]),
                              datetime.fromisoformat(part["end"])),
              "format": "geojson", "orderby": "time-asc"}
    resp = get(session, src["url"], params=params)
    payload = json.loads(resp.content)
    return Extraction(SOURCE_ID, str(resp.url), {**params, "vnum": part["vnum"]},
                      resp.status_code, resp.content, payload)


def write_bronze_unique(envelope: dict, directory: Path) -> Path | None:
    """Write <source>_YYYYMMDD_HHMMSS.json without ever overwriting a file.

    Backfill tasks run in parallel and can finish in the same second, so the name is reserved
    with an exclusive create of the .part file; if the final name is taken, move one second on.
    Empty partitions are not written.
    """
    if not envelope.get("record_count"):
        return None
    directory.mkdir(parents=True, exist_ok=True)
    when = datetime.fromisoformat(envelope["retrieved_utc"])
    body = json.dumps(envelope, ensure_ascii=False)
    for _ in range(3600):
        path = directory / landing_name(envelope["source_id"], "json", when)
        part = path.with_suffix(".json.part")
        try:
            fh = open(part, "x", encoding="utf-8")    # another task is writing this name
        except FileExistsError:
            when += timedelta(seconds=1)
            continue
        with fh:
            taken = path.exists()                    # written earlier by another task
            if not taken:
                fh.write(body)
        if taken:
            part.unlink(missing_ok=True)
            when += timedelta(seconds=1)
            continue
        part.replace(path)
        return path
    raise RuntimeError("no free Bronze file name found")


def run_fdsn_backfill(volcano_coords: dict[int, tuple[float, float]], cfg: dict, session,
                      bronze_dir: Path, start: datetime, end: datetime) -> dict:
    """Sequential version of the bronze_backfill_fdsn DAG (used by scripts/run_pipeline_local.py)."""
    from .envelope import build_envelope
    from .validate import validate_payload

    parts = volcano_partitions(volcano_coords, cfg, session, start, end)
    events = files = 0
    for i, part in enumerate(parts, 1):
        ex = fetch_partition(part, cfg, session)
        errors = validate_payload(ex.source_id, ex.payload)
        if errors:
            raise ValueError(f"vnum {part['vnum']}: {errors}")
        envelope = build_envelope(ex.source_id, ex.url, ex.params, ex.http_status, ex.content,
                                  ex.payload, datetime.now(timezone.utc))
        path = write_bronze_unique(envelope, bronze_dir)
        advance_watermark(bronze_dir, ex.payload)    # the incremental mode starts from here
        events += envelope["record_count"]
        files += path is not None
        log.info("partition %d/%d vnum %s %s -> %s: %d events", i, len(parts), part["vnum"],
                 part["start"][:10], part["end"][:10], envelope["record_count"])
    return {"partitions": len(parts), "events": events, "files": files}
