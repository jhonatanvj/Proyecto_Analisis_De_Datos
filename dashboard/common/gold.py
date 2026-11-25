"""Load the latest Gold summary for a dashboard.

The Gold DAG writes datalake_gold/<kind>_YYYYMMDD_HHMMSS.parquet (kind = governance or
storytelling) in the long format of src/volcano_pipeline/gold/contract.py. The dashboards read
only these files, with pandas, as the workshop requires.

load_gold() is called on every refresh, so it keeps the last snapshot per kind and only reads the
Parquet file again when a newer file (or a rewritten one) shows up. A file that cannot be read or
lacks a contract column is logged once and skipped, and the next older file is used instead, so
one bad file never stops a dashboard.
"""
from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

GOLD_COLUMNS = ["section", "metric", "dimension", "period", "value", "unit", "generated_utc"]
KINDS = ("governance", "storytelling")
ROOT = Path(__file__).resolve().parents[2]

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class GoldSnapshot:
    path: Path | None
    frame: pd.DataFrame
    generated_utc: pd.Timestamp | None

    def section(self, name: str) -> pd.DataFrame:
        return self.frame[self.frame["section"] == name].reset_index(drop=True)


# kind -> ((path, mtime_ns), snapshot); _bad holds (path, mtime_ns) of files that failed to load
_cache: dict[str, tuple[tuple[Path, int], GoldSnapshot]] = {}
_bad: set[tuple[Path, int]] = set()


def gold_dir() -> Path:
    return Path(os.getenv("DATALAKE_ROOT", ROOT)) / "datalake_gold"


def _check_kind(kind: str) -> None:
    if kind not in KINDS:
        raise ValueError(f"unknown Gold kind {kind!r}, expected one of {KINDS}")


def gold_files(kind: str, folder: Path | None = None) -> list[Path]:
    """Every <kind>_YYYYMMDD_HHMMSS.parquet, newest first (the stamp sorts as text).

    Spark's temporary .<kind>_<stamp>.tmp folders and files of the other kind never match."""
    _check_kind(kind)
    folder = gold_dir() if folder is None else Path(folder)
    if not folder.is_dir():
        return []
    pattern = re.compile(rf"^{kind}_\d{{8}}_\d{{6}}\.parquet$")
    names = sorted((p.name for p in folder.iterdir() if p.is_file() and pattern.match(p.name)), reverse=True)
    return [folder / name for name in names]


def latest_gold_file(kind: str, folder: Path | None = None) -> Path | None:
    files = gold_files(kind, folder)
    return files[0] if files else None


def _empty() -> GoldSnapshot:
    return GoldSnapshot(None, pd.DataFrame(columns=GOLD_COLUMNS), None)


def _read(path: Path) -> GoldSnapshot:
    frame = pd.read_parquet(path)
    missing = [c for c in GOLD_COLUMNS if c not in frame.columns]
    if missing:
        raise ValueError(f"missing Gold columns {missing}")
    frame = frame[GOLD_COLUMNS]
    generated = frame["generated_utc"].max() if not frame.empty else None
    return GoldSnapshot(path, frame, None if pd.isna(generated) else pd.Timestamp(generated))


def load_gold(kind: str, folder: Path | None = None) -> GoldSnapshot:
    """Snapshot of the newest readable Gold file of this kind, or an empty one if there is none."""
    for path in gold_files(kind, folder):
        try:
            key = (path, path.stat().st_mtime_ns)
        except FileNotFoundError:          # removed between listing and reading
            continue
        cached = _cache.get(kind)
        if cached and cached[0] == key:
            return cached[1]
        if key in _bad:
            continue
        try:
            snapshot = _read(path)
        except Exception as exc:           # corrupt, truncated or not in the Gold contract
            _bad.add(key)
            log.warning("skipping unreadable Gold file %s: %s", path.name, exc)
            continue
        _cache[kind] = (key, snapshot)
        return snapshot
    return _empty()
