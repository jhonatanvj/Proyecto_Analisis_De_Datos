"""Process new Bronze files into Silver Parquet files.

One call writes at most one Parquet file per topic, named with the processing time, plus one
quarantine file. Every row carries the Bronze file it came from. The manifest and the
seen-index are updated only after all files of the call were written, so a crash in the middle
leaves the Bronze files unprocessed and the next run repeats them. A journal
(_state/pending.json) lists the files a call is about to write; if the call dies before the state
is saved, the next call deletes those files first, so the repeat cannot duplicate rows.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from ..normalization.canonical import content_hash
from .io import topic_path, write_quarantine, write_topic
from .state import filter_unseen, load_manifest, mark_processed, update_seen
from .transform import TRANSFORMS, build_context, hans_volcano_map

log = logging.getLogger(__name__)

# Bronze sources that only feed the transform context (lookups), never a Silver topic
REFERENCE_SOURCES = {"usgs_hans_volcanoes"}
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


def _latest_envelope(bronze_dir: Path, source_id: str) -> dict | None:
    names = [p for p in bronze_dir.glob(f"{source_id}_*.json") if re.fullmatch(rf"{source_id}_\d{{8}}_\d{{6}}\.json", p.name)]
    for path in reversed(sorted(names, key=_timestamp_key)):
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except ValueError:
            continue
    return None


def _context(bronze_dir: Path, radius_km: float, min_magnitude: float | None) -> dict:
    ctx = build_context(_latest_envelope(bronze_dir, "gvp_volcanoes"), radius_km, min_magnitude)
    # volcano code -> vnum / region for the HANS notice texts
    ctx["hans_volcanoes"] = hans_volcano_map(_latest_envelope(bronze_dir, "usgs_hans_volcanoes"))
    return ctx


def _output_paths(silver_dir: Path, topics: list[str], when: datetime) -> list[Path]:
    stamp = f"{when:%Y%m%d_%H%M%S}"
    return ([topic_path(silver_dir, t, when) for t in topics]
            + [silver_dir / "quarantine" / f"rejected_{stamp}.parquet"])


def _write_journal(silver_dir: Path, topics: list[str], when: datetime) -> None:
    sdir = state_dir(silver_dir)
    sdir.mkdir(parents=True, exist_ok=True)
    names = [p.relative_to(silver_dir).as_posix() for p in _output_paths(silver_dir, topics, when)]
    (sdir / "pending.json").write_text(json.dumps(names), encoding="utf-8")


def _rollback_pending(silver_dir: Path) -> None:
    """Delete the outputs of a call that died before saving its state."""
    journal = state_dir(silver_dir) / "pending.json"
    if not journal.exists():
        return
    for name in json.loads(journal.read_text(encoding="utf-8")):
        (silver_dir / name).unlink(missing_ok=True)
        log.warning("removed orphan Silver file %s from an interrupted run", name)
    journal.unlink()


def _new_rejects(rejects: list[dict], sdir: Path) -> tuple[list[dict], pd.DataFrame]:
    """The feed is a rolling 7-day window, so the same bad record comes back every hour.
    Quarantine each (source, reason, record) only once."""
    if not rejects:
        return [], pd.DataFrame(columns=["record_id", "content_hash"])
    keys = [f"{r.get('source_id')}:{r['reason']}:{content_hash(r.get('payload'))}" for r in rejects]
    df = pd.DataFrame({"record_id": keys, "content_hash": keys, "pos": range(len(keys))})
    fresh = filter_unseen(df, sdir, "quarantine")
    keep = set(fresh["pos"])
    return [r for i, r in enumerate(rejects) if i in keep], fresh[["record_id", "content_hash"]]


def process_bronze_files(bronze_dir: Path, silver_dir: Path, files: list[Path] | None = None,
                         when: datetime | None = None, radius_km: float = 30.0,
                         min_magnitude: float | None = 1.0) -> dict:
    when = (when or datetime.now(timezone.utc)).astimezone(timezone.utc)
    files = unprocessed_files(bronze_dir, silver_dir) if files is None else sorted(files, key=_timestamp_key)
    ctx = _context(bronze_dir, radius_km, min_magnitude)
    frames: dict[str, list[pd.DataFrame]] = {}
    rejects: list[dict] = []
    out_of_scope = 0

    _rollback_pending(silver_dir)
    for path in files:
        try:
            raw = path.read_bytes()
        except OSError as exc:
            rejects.append({"source_id": None, "reason": "unreadable_bronze",
                            "payload": repr(exc), "bronze_file": path.name})
            continue
        lineage = {"bronze_file": path.name, "bronze_sha256": hashlib.sha256(raw).hexdigest(),
                   "processed_utc": pd.Timestamp(when)}
        try:
            envelope = json.loads(raw.decode("utf-8"))
            source_id = envelope["source_id"]
        except (ValueError, KeyError, TypeError):
            rejects.append({"source_id": None, "reason": "unreadable_bronze",
                            "payload": raw[:2000].decode("utf-8", "replace"), "bronze_file": path.name})
            continue
        if source_id in REFERENCE_SOURCES:
            continue        # read through _context(), not a Silver topic: processed, nothing to write
        transform = TRANSFORMS.get(source_id)
        if transform is None:
            rejects.append({"source_id": source_id, "reason": "unknown_source",
                            "payload": {"source_id": source_id}, "bronze_file": path.name})
            continue
        try:
            topic, df, bad = transform(envelope, ctx)
        except Exception as exc:   # one bad file must not block every later run
            log.exception("transform failed for %s", path.name)
            rejects.append({"source_id": source_id, "reason": "transform_error",
                            "payload": f"{type(exc).__name__}: {exc}", "bronze_file": path.name})
            continue
        out_of_scope += df.attrs.get("out_of_scope", 0)
        rejects += [{**r, "bronze_file": path.name} for r in bad]
        if not df.empty:
            frames.setdefault(topic, []).append(df.assign(**lineage))

    sdir = state_dir(silver_dir)
    written, fresh_by_topic = {}, {}
    _write_journal(silver_dir, list(frames), when)
    for topic, parts in frames.items():
        fresh = filter_unseen(pd.concat(parts, ignore_index=True), sdir, topic)
        path = write_topic(fresh, silver_dir, topic, when)
        if path is not None:
            written[topic] = path.relative_to(silver_dir).as_posix()
            fresh_by_topic[topic] = fresh
    rejects, rejects_seen = _new_rejects(rejects, sdir)
    write_quarantine(rejects, silver_dir, when)

    for topic, fresh in fresh_by_topic.items():
        update_seen(fresh, sdir, topic)
    update_seen(rejects_seen, sdir, "quarantine")
    mark_processed(sdir, [p.name for p in files])
    (sdir / "pending.json").unlink(missing_ok=True)

    summary = {"processed": [p.name for p in files], "written": written,
               "rows": {t: len(f) for t, f in fresh_by_topic.items()},
               "rejected": len(rejects), "out_of_scope": out_of_scope}
    log.info(json.dumps({"event": "silver_run", **summary}))
    return summary
