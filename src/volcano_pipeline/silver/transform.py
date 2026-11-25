"""Bronze envelope -> clean Silver rows, one function per source.

Each function returns ``(topic, dataframe, rejects)``. Rejects are records that fail validation
and go to quarantine with a reason. The rules come from the Workshop 1 profiling findings; see
workshop_2/preprocessing.md for the justification of each one.
"""
from __future__ import annotations

from typing import Callable

import numpy as np
import pandas as pd
from dateutil import parser as dateparser

from ..normalization.canonical import content_hash, gvp_eruption_date
from ..normalization.geo import EARTH_RADIUS_KM, normalize_longitude
from ..sources import hans_notices
from ..sources.usgs_hans import ALERT_LEVELS, reject_reason, valid_vnum
from .schemas import enforce
from .quality import iqr_outliers, robust_z_outliers
from .text import clean_text, keyword_flags, load_stopwords, tokenize

DEPTH_RANGE = (-5.0, 800.0)      # negative depth = above sea level, normal at volcanoes
MAG_RANGE = (-2.0, 10.0)

# time zones used in USGS observatory notices (dateutil does not know them)
US_TZ = {"UTC": 0, "HST": -36000, "AKST": -32400, "AKDT": -28800, "PST": -28800, "PDT": -25200,
         "MST": -25200, "MDT": -21600, "ChST": 36000}

Transform = Callable[[dict, dict], "tuple[str, pd.DataFrame, list[dict]]"]


def build_context(volcanoes_envelope: dict | None, radius_km: float = 30.0,
                  min_magnitude: float | None = 1.0) -> dict:
    """Volcano positions from the newest gvp_volcanoes Bronze file (empty when there is none)."""
    coords: dict[int, tuple[float, float]] = {}
    for feat in ((volcanoes_envelope or {}).get("payload") or {}).get("features", []):
        p = feat.get("properties") or {}
        try:
            coords[int(p["Volcano_Number"])] = (float(p["Latitude"]), float(p["Longitude"]))
        except (KeyError, TypeError, ValueError):
            continue
    return {"volcano_coords": coords, "radius_km": radius_km, "min_magnitude": min_magnitude}


def parse_us_time(text: str | None) -> pd.Timestamp | None:
    if not text:
        return None
    try:
        dt = dateparser.parse(text, tzinfos=US_TZ, fuzzy=True)
    except (ValueError, OverflowError):
        return None
    ts = pd.Timestamp(dt)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def _reject(source_id: str, reason: str, payload) -> dict:
    return {"source_id": source_id, "reason": reason, "payload": payload}


def nearest_volcanoes(lats, lons, coords: dict[int, tuple[float, float]],
                      chunk: int = 20_000) -> tuple[np.ndarray, np.ndarray]:
    """Nearest volcano and its great-circle distance (km) for every point, vectorised.

    The FDSN backfill brings ~365k events and there are 165 volcanoes, so a Python loop over
    every pair would take minutes; numpy does it in a fraction of a second per chunk.
    """
    vnums = np.array(list(coords.keys()))
    vlat = np.radians([c[0] for c in coords.values()])[None, :]
    vlon = np.radians([c[1] for c in coords.values()])[None, :]
    lat = np.radians(np.asarray(lats, dtype=float))
    lon = np.radians(np.asarray(lons, dtype=float))
    best_v = np.empty(len(lat), dtype=vnums.dtype)
    best_d = np.empty(len(lat), dtype=float)
    for i in range(0, len(lat), chunk):
        la, lo = lat[i:i + chunk, None], lon[i:i + chunk, None]
        a = np.sin((vlat - la) / 2) ** 2 + np.cos(la) * np.cos(vlat) * np.sin((vlon - lo) / 2) ** 2
        d = 2 * EARTH_RADIUS_KM * np.arcsin(np.sqrt(np.clip(a, 0, 1)))
        idx = d.argmin(axis=1)
        best_v[i:i + chunk] = vnums[idx]
        best_d[i:i + chunk] = d[np.arange(len(idx)), idx]
    return best_v, best_d


def earthquakes_from_feed(envelope: dict, ctx: dict) -> tuple[str, pd.DataFrame, list[dict]]:
    src, rows, rejects, out_of_scope = envelope["source_id"], [], [], 0
    coords, radius = ctx.get("volcano_coords") or {}, ctx.get("radius_km", 30.0)
    min_mag = ctx.get("min_magnitude")
    for feat in (envelope.get("payload") or {}).get("features", []):
        p = feat.get("properties") or {}
        geom = (feat.get("geometry") or {}).get("coordinates")
        if not feat.get("id"):
            rejects.append(_reject(src, "missing_id", feat))
            continue
        if not geom or len(geom) < 2 or geom[0] is None or geom[1] is None:
            rejects.append(_reject(src, "missing_coordinates", feat))
            continue
        if p.get("type") != "earthquake":
            rejects.append(_reject(src, "non_tectonic_type", feat))
            continue
        lon, lat = normalize_longitude(float(geom[0])), float(geom[1])
        depth = geom[2] if len(geom) > 2 else None
        mag = p.get("mag")
        if not (-90 <= lat <= 90):
            rejects.append(_reject(src, "latitude_out_of_range", feat))
            continue
        if depth is not None and not (DEPTH_RANGE[0] <= depth <= DEPTH_RANGE[1]):
            rejects.append(_reject(src, "depth_out_of_range", feat))
            continue
        if mag is not None and not (MAG_RANGE[0] <= mag <= MAG_RANGE[1]):
            rejects.append(_reject(src, "magnitude_out_of_range", feat))
            continue
        rows.append({
            "record_id": f"usgs:{feat['id']}", "content_hash": content_hash(feat),
            "event_time": pd.to_datetime(p.get("time"), unit="ms", utc=True),
            "updated_time": pd.to_datetime(p.get("updated"), unit="ms", utc=True),
            "latitude": lat, "longitude": lon, "depth_km": depth, "mag": mag,
            "mag_type": (p.get("magType") or "").lower() or None, "network": p.get("net"),
            "review_status": p.get("status"), "place": p.get("place"),
            "mag_missing": mag is None,
        })
    df = pd.DataFrame(rows)
    if not df.empty and coords:
        vnums, dists = nearest_volcanoes(df["latitude"], df["longitude"], coords)
        df["nearest_vnum"], df["distance_km"] = vnums, np.round(dists, 2)
        keep = (df["distance_km"] <= radius)
        if min_mag is not None:
            keep &= df["mag"].isna() | (df["mag"] >= min_mag)
        out_of_scope = int((~keep).sum())       # counted, not stored (Workshop 1 contract)
        df = df[keep]
    if not df.empty:
        df = df.sort_values("updated_time").drop_duplicates("record_id", keep="last")
        for col, name in (("mag", "mag"), ("depth_km", "depth")):
            df[f"is_outlier_{name}_iqr"] = iqr_outliers(df[col])
            df[f"is_outlier_{name}_z"] = robust_z_outliers(df[col])
    df.attrs["out_of_scope"] = out_of_scope
    return "earthquakes", enforce(df, "earthquakes"), rejects


def alert_notices_from_hans(envelope: dict, ctx: dict) -> tuple[str, pd.DataFrame, list[dict]]:
    src, rows, rejects = envelope["source_id"], [], []
    coords = ctx.get("volcano_coords") or {}
    for r in envelope.get("payload") or []:
        reason = reject_reason(r)
        if reason:
            rejects.append(_reject(src, reason, r))
            continue
        vnum = int(r["vnum"])
        level = r["alert_level"].upper()
        lat, lon = coords.get(vnum, (None, None))
        rows.append({
            "record_id": f"usgs_hans:{vnum}:{r['notice_identifier']}", "content_hash": content_hash(r),
            "vnum": vnum, "volcano_name": (r.get("volcano_name") or "").strip() or None,
            "observatory": (r.get("obs_abbr") or "").lower() or None,
            "alert_level": level, "alert_level_ord": ALERT_LEVELS[level],
            "color_code": (r.get("color_code") or "").upper() or None,
            "notice_identifier": r["notice_identifier"],
            "sent_time": pd.to_datetime(r["sent_unixtime"], unit="s", utc=True),
            "latitude": lat, "longitude": lon, "coords_missing": lat is None,
        })
    return "alert_notices", enforce(pd.DataFrame(rows), "alert_notices"), rejects


def volcanoes_from_gvp(envelope: dict, ctx: dict) -> tuple[str, pd.DataFrame, list[dict]]:
    """GVP volcanoes, with the USGS observatory and region from the HANS volcano list when it has them."""
    src, rows, rejects = envelope["source_id"], [], []
    hans = ctx.get("hans_by_vnum") or {}
    for feat in (envelope.get("payload") or {}).get("features", []):
        p = feat.get("properties") or {}
        geom = (feat.get("geometry") or {}).get("coordinates") or [None, None]
        lat, lon = p.get("Latitude", geom[1]), p.get("Longitude", geom[0])
        if p.get("Volcano_Number") is None or lat is None or lon is None:
            rejects.append(_reject(src, "missing_vnum_or_coordinates", p))
            continue
        rows.append({
            "record_id": f"gvp_volcano:{p['Volcano_Number']}", "content_hash": content_hash(p),
            "vnum": p["Volcano_Number"], "name": (p.get("Volcano_Name") or "").strip() or None,
            "country": p.get("Country"), "primary_type": p.get("Primary_Volcano_Type"),
            # 'Unknown' in older exports, null in the WFS: both become NULL
            "last_eruption_year": pd.to_numeric(p.get("Last_Eruption_Year"), errors="coerce"),
            "elevation_m": p.get("Elevation"),
            "latitude": float(lat), "longitude": normalize_longitude(float(lon)),
            "subregion": p.get("Subregion"),
            "observatory": hans.get(p["Volcano_Number"], {}).get("observatory"),
            "usgs_region": hans.get(p["Volcano_Number"], {}).get("region"),
        })
    return "volcanoes", enforce(pd.DataFrame(rows), "volcanoes"), rejects


def eruptions_from_gvp(envelope: dict, ctx: dict) -> tuple[str, pd.DataFrame, list[dict]]:
    src, rows, rejects = envelope["source_id"], [], []
    us_vnums = set((ctx.get("volcano_coords") or {}).keys())
    for feat in (envelope.get("payload") or {}).get("features", []):
        p = feat.get("properties") or {}
        geom = (feat.get("geometry") or {}).get("coordinates") or [None, None]
        if p.get("Eruption_Number") is None or p.get("Volcano_Number") is None:
            rejects.append(_reject(src, "missing_eruption_or_volcano_number", p))
            continue
        if us_vnums and p["Volcano_Number"] not in us_vnums:
            continue        # the eruptions layer has no country field: scope by volcano number
        ts, precision = gvp_eruption_date(p.get("StartDateYear"), p.get("StartDateMonth"), p.get("StartDateDay"))
        end_ts, end_precision = gvp_eruption_date(p.get("EndDateYear"), p.get("EndDateMonth"), p.get("EndDateDay"))
        vei = p.get("ExplosivityIndexMax")
        rows.append({
            "record_id": f"gvp_eruption:{p['Eruption_Number']}", "content_hash": content_hash(p),
            "eruption_number": p["Eruption_Number"], "vnum": p["Volcano_Number"],
            "start_time": ts, "time_precision": precision, "start_year": p.get("StartDateYear"),
            "vei": vei, "vei_missing": vei is None, "activity_type": p.get("Activity_Type"),
            # GVP writes '>' before the end year of an eruption that was still going on at that date
            "end_time": end_ts, "end_precision": end_precision,
            "is_continuing": (p.get("EndDateYearModifier") or "").strip() == ">",
            "latitude": geom[1], "longitude": normalize_longitude(geom[0]) if geom[0] is not None else None,
        })
    return "eruptions", enforce(pd.DataFrame(rows), "eruptions"), rejects


def volcano_updates_from_scrape(envelope: dict, ctx: dict) -> tuple[str, pd.DataFrame, list[dict]]:
    src, rows, rejects = envelope["source_id"], [], []
    stopwords = load_stopwords()
    for r in envelope.get("payload") or []:
        if r.get("record_type") not in ("elevated", "observatory_notice"):
            rejects.append(_reject(src, "unknown_record_type", r))
            continue
        text = clean_text(r.get("text_html"))
        if not text:
            rejects.append(_reject(src, "empty_text", r))
            continue
        tokens = tokenize(text, stopwords)
        key = r.get("notice_url") or r.get("notice_number") or r.get("issued_raw") or r.get("as_of_raw")
        rows.append({
            "record_id": f"usgs_vu:{r['record_type']}:{r.get('observatory')}:{r.get('volcano_name')}:{key}",
            "content_hash": content_hash(r),
            "record_type": r["record_type"], "observatory": r.get("observatory"),
            "volcano_name": r.get("volcano_name"), "vnum": r.get("vnum"),
            "alert_level": r.get("alert_level"), "color_code": r.get("color_code"),
            "as_of_time": parse_us_time(r.get("as_of_raw") or r.get("issued_raw")),
            "issued_raw": r.get("issued_raw"), "notice_number": r.get("notice_number"),
            "notice_url": r.get("notice_url"), "text_clean": text, "tokens": tokens,
            "token_count": len(tokens), **keyword_flags(tokens),
        })
    return "volcano_updates", enforce(pd.DataFrame(rows), "volcano_updates"), rejects


def hans_volcano_map(envelope: dict | None) -> dict[str, dict]:
    """USGS volcano code (hi3, ak111...) -> vnum, name and region, from getUSVolcanoes."""
    out = {}
    for r in (envelope or {}).get("payload") or []:
        code = (r.get("volcano_cd") or "").strip().lower()
        if code and valid_vnum(r.get("vnum")):
            out[code] = {"vnum": int(r["vnum"]), "name": r.get("volcano_name"), "region": r.get("region")}
    return out


def hans_volcanoes_by_vnum(envelope: dict | None) -> dict[int, dict]:
    """vnum -> USGS observatory (avo, hvo, cvo, calvo, yvo, nmi) and region, from getUSVolcanoes."""
    out = {}
    for r in (envelope or {}).get("payload") or []:
        if valid_vnum(r.get("vnum")):
            out[int(r["vnum"])] = {"observatory": (r.get("obs_abbr") or "").strip().lower() or None,
                                   "region": r.get("region")}
    return out


def notices_from_hans_search(envelope: dict, ctx: dict) -> tuple[str, pd.DataFrame, list[dict]]:
    """Full notice texts. The summary paragraph and the body (footer and link lines removed) are
    cleaned like the scraped notices; volcano codes map to vnum/region through getUSVolcanoes, and
    the VNUMs written in the text fill the gap when a code is unknown."""
    src, rows, rejects = envelope["source_id"], [], []
    stopwords = load_stopwords()
    codes_map = ctx.get("hans_volcanoes") or {}
    for r in envelope.get("payload") or []:
        sent = hans_notices.sent_time(r)
        if not r.get("noticeIdentifier") or sent is None:
            rejects.append(_reject(src, "missing_id_or_time", {k: v for k, v in r.items() if k != "noticeHtml"}))
            continue
        parsed = hans_notices.parse_notice(r.get("noticeHtml"))
        text = clean_text(parsed["body_raw"])
        if not text:
            rejects.append(_reject(src, "empty_text", {k: v for k, v in r.items() if k != "noticeHtml"}))
            continue
        codes = [c.strip().lower() for c in (r.get("volcCds") or "").split(",") if c.strip()]
        known = [codes_map[c] for c in codes if c in codes_map]
        vnums = [v["vnum"] for v in known] or parsed["vnums_in_text"]
        summary = clean_text(parsed["summary_raw"])
        tokens, summary_tokens = tokenize(text, stopwords), tokenize(summary, stopwords)
        level = parsed["alert_level"]
        rows.append({
            # one notice id is published in two formats (VAN and its VONA, 708 cases): id + type is the key
            "record_id": f"usgs_hans_notice:{r['noticeIdentifier']}:{r.get('noticeTypeCd')}",
            "content_hash": content_hash(r),
            "notice_identifier": r["noticeIdentifier"], "sent_time": pd.Timestamp(sent),
            "observatory": (r.get("obsAbbr") or "").lower() or None, "notice_type": r.get("noticeTypeCd"),
            "volcano_codes": ",".join(codes) or None, "vnums": vnums, "vnum": vnums[0] if vnums else None,
            "volcano_name": known[0]["name"] if known else None,
            "region": known[0]["region"] if known else None, "is_region_wide": not codes and not vnums,
            "alert_level": level, "alert_level_ord": ALERT_LEVELS.get(level), "color_code": parsed["color_code"],
            "summary_clean": summary or None, "summary_tokens": summary_tokens,
            "text_clean": text, "tokens": tokens, "token_count": len(tokens),
            "text_length": len(parsed["body_raw"]), "summary_length": len(parsed["summary_raw"]),
            "notice_url": r.get("permLink"), **keyword_flags(tokens),
        })
    return "notices", enforce(pd.DataFrame(rows), "notices"), rejects


TRANSFORMS: dict[str, Transform] = {
    "usgs_feed": earthquakes_from_feed,
    "usgs_fdsn": earthquakes_from_feed,      # historical backfill: same GeoJSON format and ids
    "usgs_hans": alert_notices_from_hans,
    "gvp_volcanoes": volcanoes_from_gvp,
    "gvp_eruptions": eruptions_from_gvp,
    "usgs_volcano_updates": volcano_updates_from_scrape,
    "usgs_hans_notices": notices_from_hans_search,
}
