"""Silver state kept in datalake_silver/_state/.

* manifest.json: Bronze files already processed, so a file is never processed twice.
* seen_<topic>.parquet: latest content_hash per record_id, so a record that appears again in a
  later Bronze file (the USGS feed is a rolling 7-day window) is written only if it changed.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

import pandas as pd


def load_manifest(state_dir: Path) -> set[str]:
    path = state_dir / "manifest.json"
    if not path.exists():
        return set()
    return set(json.loads(path.read_text(encoding="utf-8")))


def mark_processed(state_dir: Path, names: Iterable[str]) -> None:
    state_dir.mkdir(parents=True, exist_ok=True)
    done = load_manifest(state_dir) | set(names)
    tmp = state_dir / "manifest.json.part"
    tmp.write_text(json.dumps(sorted(done), indent=1), encoding="utf-8")
    tmp.replace(state_dir / "manifest.json")


def _seen_path(state_dir: Path, topic: str) -> Path:
    return state_dir / f"seen_{topic}.parquet"


def _load_seen(state_dir: Path, topic: str) -> pd.Series:
    path = _seen_path(state_dir, topic)
    if not path.exists():
        return pd.Series(dtype=object)
    seen = pd.read_parquet(path)
    return seen.set_index("record_id")["content_hash"]


def filter_unseen(df: pd.DataFrame, state_dir: Path, topic: str) -> pd.DataFrame:
    """Rows that are new or changed; within the batch the last row per record_id wins."""
    if df.empty:
        return df
    latest = df.drop_duplicates(subset="record_id", keep="last")
    seen = _load_seen(state_dir, topic)
    known = latest["record_id"].map(seen)
    return latest[known.isna() | (known != latest["content_hash"])]


def update_seen(df: pd.DataFrame, state_dir: Path, topic: str) -> None:
    if df.empty:
        return
    state_dir.mkdir(parents=True, exist_ok=True)
    seen = _load_seen(state_dir, topic)
    new = df.drop_duplicates(subset="record_id", keep="last").set_index("record_id")["content_hash"]
    merged = pd.concat([seen[~seen.index.isin(new.index)], new])
    out = merged.rename("content_hash").rename_axis("record_id").reset_index()
    tmp = _seen_path(state_dir, topic).with_suffix(".part")
    out.to_parquet(tmp, index=False)
    tmp.replace(_seen_path(state_dir, topic))
