"""Raw landing zone: keep every response exactly as received.

Files are named ``<source_id>_YYYYMMDD_HHMMSS.<ext>`` (UTC) as required by the
course reference architecture, and a sidecar ``.meta.json`` stores the request
URL, HTTP headers relevant for change detection and the SHA-256 content hash.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def landing_name(source_id: str, ext: str, when: datetime | None = None) -> str:
    when = (when or datetime.now(timezone.utc)).astimezone(timezone.utc)
    return f"{source_id}_{when:%Y%m%d_%H%M%S}.{ext.lstrip('.')}"


def save_raw(
    content: bytes,
    source_id: str,
    ext: str,
    directory: Path,
    url: str = "",
    headers: dict | None = None,
    when: datetime | None = None,
    suffix: str = "",
) -> Path:
    """Write content unmodified and its metadata sidecar. Returns the data path."""
    directory.mkdir(parents=True, exist_ok=True)
    when = when or datetime.now(timezone.utc)
    name = landing_name(source_id + (f"_{suffix}" if suffix else ""), ext, when)
    path = directory / name
    path.write_bytes(content)
    keep = {k: v for k, v in (headers or {}).items()
            if k.lower() in {"etag", "last-modified", "content-type", "content-length", "date"}}
    meta = {
        "source_id": source_id,
        "retrieved_utc": when.astimezone(timezone.utc).isoformat(),
        "url": url,
        "bytes": len(content),
        "sha256": sha256_bytes(content),
        "headers": keep,
    }
    path.with_suffix(path.suffix + ".meta.json").write_text(json.dumps(meta, indent=2))
    return path
