"""Bronze extraction of the HANS notice texts: incremental (watermark -> now) and backfill.

Incremental: each hourly run asks the HANS search for the notices sent since the watermark minus
``overlap_hours`` and writes them as one Bronze file. The watermark is the newest ``sentUnixtime``
already stored in Bronze, kept in ``datalake_bronze/_state/usgs_hans_notices.watermark``. It is
advanced only after the Bronze file was written, so a failed run fetches the same interval again.
Notices fetched twice (overlap) have the same id and content hash, and Silver drops them.

Backfill: the archive since ``historical_start`` is read one calendar month per Bronze file,
oldest first, with a pause between pages. A month already present (its file exists in Bronze) is
skipped, so an interrupted backfill can simply be started again.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

from ..config import get_source
from ..sources import hans_notices
from .envelope import build_envelope
from .extract import Extraction

log = logging.getLogger(__name__)

SOURCE_ID = "usgs_hans_notices"


def _state_file(bronze_dir: Path) -> Path:
    return bronze_dir / "_state" / f"{SOURCE_ID}.watermark"


def read_watermark(bronze_dir: Path) -> datetime | None:
    path = _state_file(bronze_dir)
    if not path.exists():
        return None
    try:
        return datetime.fromisoformat(path.read_text(encoding="utf-8").strip())
    except ValueError:
        return None


def advance_watermark(bronze_dir: Path, notices: list[dict]) -> datetime | None:
    """Move the watermark to the newest notice in ``notices`` (never backwards)."""
    times = [t for t in (hans_notices.sent_time(n) for n in notices) if t is not None]
    current = read_watermark(bronze_dir)
    if not times:
        return current
    newest = max(times)
    if current is not None and newest <= current:
        return current
    path = _state_file(bronze_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".part")
    tmp.write_text(newest.isoformat(), encoding="utf-8")
    tmp.replace(path)
    return newest


def incremental_window(cfg: dict, bronze_dir: Path, now: datetime) -> tuple[datetime, datetime]:
    src = get_source(cfg, SOURCE_ID)
    mark = read_watermark(bronze_dir)
    if mark is None:
        return now - timedelta(days=int(src.get("first_window_days", 30))), now
    return mark - timedelta(hours=float(src.get("overlap_hours", 6))), now


def _extraction(cfg: dict, session, start: datetime, end: datetime) -> Extraction:
    src = get_source(cfg, SOURCE_ID)
    notices, raw, status = hans_notices.fetch_window(session, src["url"], start, end,
                                                     float(src.get("page_pause_seconds", 1.0)))
    params = {"startUnixtime": int(start.timestamp()), "endUnixtime": int(end.timestamp()),
              "window_start": start.isoformat(), "window_end": end.isoformat()}
    return Extraction(SOURCE_ID, hans_notices.search_url(src["url"]), params, status, raw, notices)


def extract_incremental(cfg: dict, session, bronze_dir: Path, now: datetime | None = None) -> Extraction:
    now = now or datetime.now(timezone.utc)
    start, end = incremental_window(cfg, bronze_dir, now)
    return _extraction(cfg, session, start, end)


def month_windows(start: datetime, end: datetime) -> list[tuple[datetime, datetime]]:
    windows, cur = [], datetime(start.year, start.month, 1, tzinfo=timezone.utc)
    while cur < end:
        nxt = datetime(cur.year + (cur.month == 12), cur.month % 12 + 1, 1, tzinfo=timezone.utc)
        windows.append((max(cur, start), min(nxt, end)))
        cur = nxt
    return windows


def _done_months(bronze_dir: Path) -> set[str]:
    path = bronze_dir / "_state" / f"{SOURCE_ID}.backfill_done"
    return set(path.read_text(encoding="utf-8").split()) if path.exists() else set()


def _mark_month(bronze_dir: Path, key: str) -> None:
    path = bronze_dir / "_state" / f"{SOURCE_ID}.backfill_done"
    path.parent.mkdir(parents=True, exist_ok=True)
    done = _done_months(bronze_dir) | {key}
    path.write_text("\n".join(sorted(done)), encoding="utf-8")


def run_backfill(cfg: dict, session, bronze_dir: Path, start: datetime, end: datetime,
                 writer=None) -> dict:
    """Load the notice archive month by month. ``writer`` defaults to write_bronze_unique."""
    from .fdsn_backfill import write_bronze_unique
    from .validate import validate_payload

    writer = writer or write_bronze_unique
    done = _done_months(bronze_dir)
    months = notices = files = 0
    for w_start, w_end in month_windows(start, end):
        key = f"{w_start:%Y-%m-%d}/{w_end:%Y-%m-%d}"
        if key in done:
            continue
        ex = _extraction(cfg, session, w_start, w_end)
        errors = validate_payload(SOURCE_ID, ex.payload)
        if errors:
            raise ValueError(f"{key}: {errors}")
        envelope = build_envelope(SOURCE_ID, ex.url, ex.params, ex.http_status, ex.content, ex.payload,
                                  datetime.now(timezone.utc))
        path = writer(envelope, bronze_dir)
        advance_watermark(bronze_dir, ex.payload)
        _mark_month(bronze_dir, key)
        months += 1
        notices += len(ex.payload)
        files += path is not None
        log.info(json.dumps({"event": "hans_notices_backfill_month", "window": key,
                             "notices": len(ex.payload), "file": path.name if path else None}))
    return {"months": months, "notices": notices, "files": files}
