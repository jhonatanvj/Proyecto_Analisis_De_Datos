"""Bronze files: one JSON envelope per source and run, named <source_id>_YYYYMMDD_HHMMSS.json.

The envelope keeps the payload as received (parsed JSON, or the records extracted by the
scraper) together with where and when it came from and the SHA-256 of the original bytes.
If the content did not change since the latest file of the same source, nothing is written,
so running the DAG twice cannot create two copies of the same data.
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..storage.raw_landing import landing_name, sha256_bytes
from .validate import count_records


def build_envelope(source_id: str, url: str, params: dict, http_status: int,
                   content: bytes, payload: Any, when: datetime) -> dict:
    return {
        "source_id": source_id,
        "retrieved_utc": when.astimezone(timezone.utc).isoformat(),
        "url": url,
        "params": params or {},
        "http_status": http_status,
        "content_sha256": sha256_bytes(content),
        "record_count": count_records(source_id, payload),
        "payload": payload,
    }


def bronze_files(directory: Path, source_id: str) -> list[Path]:
    pattern = re.compile(rf"^{re.escape(source_id)}_\d{{8}}_\d{{6}}\.json$")
    if not directory.exists():
        return []
    return sorted(p for p in directory.iterdir() if pattern.match(p.name))


def latest_bronze(directory: Path, source_id: str) -> Path | None:
    files = bronze_files(directory, source_id)
    return files[-1] if files else None


def _sha_of(path: Path) -> str | None:
    try:
        return json.loads(path.read_text(encoding="utf-8")).get("content_sha256")
    except (OSError, ValueError):
        return None


# incremental window sources: an empty window means "nothing new", not a file to keep
SKIP_EMPTY = {"usgs_hans_notices", "usgs_fdsn"}


def write_bronze(envelope: dict, directory: Path) -> Path | None:
    if envelope["source_id"] in SKIP_EMPTY and not envelope.get("record_count"):
        return None
    directory.mkdir(parents=True, exist_ok=True)
    previous = latest_bronze(directory, envelope["source_id"])
    if previous is not None and _sha_of(previous) == envelope["content_sha256"]:
        return None
    when = datetime.fromisoformat(envelope["retrieved_utc"])
    path = directory / landing_name(envelope["source_id"], "json", when)
    tmp = path.with_suffix(".json.part")
    tmp.write_text(json.dumps(envelope, ensure_ascii=False, indent=1), encoding="utf-8")
    tmp.replace(path)  # the Silver sensor never sees a half-written file
    return path
