"""USGS FDSN event web service (CSV). Historical earthquakes around volcanoes."""
from __future__ import annotations

import csv
import io

import requests

from .http import get

MAX_EVENTS_PER_REQUEST = 20_000


def count(session: requests.Session, base_query_url: str, **params) -> int:
    url = base_query_url.replace("/query", "/count")
    resp = get(session, url, params={**params, "format": "text"})
    return int(resp.text.strip())


def query_radius(session: requests.Session, src: dict, lat: float, lon: float,
                 radius_km: float, start: str, end: str, min_mag: float,
                 updated_after: str | None = None) -> requests.Response:
    params = {
        **src.get("params", {}),
        "latitude": lat, "longitude": lon, "maxradiuskm": radius_km,
        "starttime": start, "endtime": end, "minmagnitude": min_mag,
    }
    if updated_after:
        params["updatedafter"] = updated_after   # incremental catch-up of revised events
    return get(session, src["url"], params=params)


def parse_csv(content: bytes) -> list[dict]:
    text = content.decode("utf-8")
    return list(csv.DictReader(io.StringIO(text)))
