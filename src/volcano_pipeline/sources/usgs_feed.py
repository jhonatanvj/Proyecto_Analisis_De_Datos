"""USGS real-time GeoJSON summary feeds (incremental)."""
from __future__ import annotations

import json

import requests

from .http import get


def fetch(session: requests.Session, url: str, etag: str | None = None,
          last_modified: str | None = None) -> requests.Response:
    return get(session, url, etag=etag, last_modified=last_modified)


def parse(content: bytes) -> tuple[dict, list[dict]]:
    data = json.loads(content)
    return data.get("metadata", {}), data.get("features", [])


def new_or_changed(features: list[dict], watermark_ms: int) -> list[dict]:
    """Features whose ``updated`` is strictly after the watermark (epoch ms)."""
    return [f for f in features if (f["properties"].get("updated") or 0) > watermark_ms]
