"""Smithsonian GVP Volcanoes of the World via GeoServer WFS (GeoJSON)."""
from __future__ import annotations

import json

import requests

from .http import get


def fetch_layer(session: requests.Session, src: dict, max_features: int | None = None) -> requests.Response:
    params = dict(src["params"])
    if max_features:
        params["maxFeatures"] = max_features          # WFS 1.0.0 paging parameter
    return get(session, src["url"], params=params)


def parse_features(content: bytes) -> list[dict]:
    """Return the list of feature property dicts (geometry coordinates added as _lon/_lat)."""
    data = json.loads(content)
    out = []
    for feat in data.get("features", []):
        props = dict(feat.get("properties") or {})
        geom = feat.get("geometry") or {}
        coords = geom.get("coordinates") or [None, None]
        props["_lon"], props["_lat"] = coords[0], coords[1]
        props["_feature_id"] = feat.get("id")
        out.append(props)
    return out
