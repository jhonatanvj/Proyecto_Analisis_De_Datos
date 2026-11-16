"""Process new Bronze files into Silver Parquet files.

One call writes at most one Parquet file per topic, named with the processing time, plus one
quarantine file. Every row carries the Bronze file it came from. The manifest and the
seen-index are updated only after all files of the call were written, so a crash in the middle
leaves the Bronze files unprocessed and the next run repeats them.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .io import write_quarantine, write_topic
from .state import filter_unseen, load_manifest, mark_processed, update_seen
from .transform import TRANSFORMS, build_context

log = logging.getLogger(__name__)

BRONZE_NAME = re.compile(r"^(?P<source>[a-z0-9_]+?)_(?P<ts>\d{8}_\d{6})\.json$")


def state_dir(silver_dir: Path) -> Path:
    return silver_dir / "_state"


def _timestamp_key(path: Path) -> tuple[str, str]:
    m = BRONZE_NAME.match(path.name)
    return (m.group("ts") if m else "", path.name)


def unprocessed_files(bronze_dir: Path, silver_dir: Path) -> list[Path]:
    """Bronze JSON files not yet in the manifest, oldest first."""
    if not bronze_dir.exists():
        return []
    done = load_manifest(state_dir(silver_dir))
    files = [p for p in bronze_dir.iterdir()
             if p.is_file() and BRONZE_NAME.match(p.name) and p.name not in done]
    return sorted(files, key=_timestamp_key)


def _context(bronze_dir: Path, radius_km: float, min_magnitude: float | None) -> dict:
    latest = sorted(bronze_dir.glob("gvp_volcanoes_*.json"), key=_timestamp_key)
    for path in reversed(latest):
        try:
            return build_context(json.loads(path.read_text(encoding="utf-8")), radius_km, min_magnitude)
        except ValueError:
            continue
    return build_context(None, radius_km, min_magnitude)


def process_bronze_files(bronze_dir: Path, silver_dir: Path, files: list[Path] | None = None,
                         when: datetime | None = None, radius_km: float = 30.0,
                         min_magnitude: float | None = 1.0) -> dict:
    when = (when or datetime.now(timezone.utc)).astimezone(timezone.utc)
    files = unprocessed_files(bronze_dir, silver_dir) if files is None else sorted(files, key=_timestamp_key)
    ctx = _context(bronze_dir, radius_km, min_magnitude)
    frames: dict[str, list[pd.DataFrame]] = {}
    rejects: list[dict] = []
    out_of_scope = 0

    for path in files:
        raw = path.read_bytes()
        lineage = {"bronze_file": path.name, "bronze_sha256": hashlib.sha256(raw).hexdigest(),
                   "processed_utc": pd.Timestamp(when)}
        try:
            envelope = json.loads(raw.decode("utf-8"))
            source_id = envelope["source_id"]
        except (ValueError, KeyError, TypeError):
            rejects.append({"source_id": None, "reason": "unreadable_bronze",
                            "payload": raw[:2000].decode("utf-8", "replace"), "bronze_file": path.name})
            continue
        transform = TRANSFORMS.get(source_id)
        if transform is None:
            rejects.append({"source_id": source_id, "reason": "unknown_source",
                            "payload": {"source_id": source_id}, "bronze_file": path.name})
            continue
        topic, df, bad = transform(envelope, ctx)
        out_of_scope += df.attrs.get("out_of_scope", 0)
        rejects += [{**r, "bronze_file": path.name} for r in bad]
        if not df.empty:
            frames.setdefault(topic, []).append(df.assign(**lineage))

    sdir = state_dir(silver_dir)
    written, fresh_by_topic = {}, {}
    for topic, parts in frames.items():
        fresh = filter_unseen(pd.concat(parts, ignore_index=True), sdir, topic)
        path = write_topic(fresh, silver_dir, topic, when)
        if path is not None:
            written[topic] = path.name
            fresh_by_topic[topic] = fresh
    write_quarantine(rejects, silver_dir, when)

    for topic, fresh in fresh_by_topic.items():
        update_seen(fresh, sdir, topic)
    mark_processed(sdir, [p.name for p in files])

    summary = {"processed": [p.name for p in files], "written": written,
               "rows": {t: len(f) for t, f in fresh_by_topic.items()},
               "rejected": len(rejects), "out_of_scope": out_of_scope}
    log.info(json.dumps({"event": "silver_run", **summary}))
    return summary
