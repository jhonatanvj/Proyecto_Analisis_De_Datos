#!/usr/bin/env python
"""Download the Workshop #1 evidence samples from the REAL sources.

Usage (from the repository root, with the virtualenv active):

    python scripts/fetch_samples.py               # first full sample run (~2-5 min)
    python scripts/fetch_samples.py --poll        # re-poll incremental sources only
    python scripts/fetch_samples.py --estimate-volume   # optional: FDSN counts per volcano (~10 min)

Run ``--poll`` again at least 1 hour later (USGS feed) and on a later day /
after the next Wednesday (weekly report) so the notebook can compare snapshots.

Small files go to workshop_1/samples/ (committed as evidence); big full
downloads go to data/raw/ (git-ignored). Every file is stored unmodified with
a .meta.json sidecar (URL, headers, SHA-256).
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from volcano_pipeline.config import get_source, load_config, raw_dir, user_agent  # noqa: E402
from volcano_pipeline.sources import gvp, usgs_fdsn, usgs_feed, usgs_hans, weekly_report  # noqa: E402
from volcano_pipeline.sources.http import get, make_session  # noqa: E402
from volcano_pipeline.storage.raw_landing import save_raw  # noqa: E402

SAMPLES = ROOT / "workshop_1" / "samples"
log = logging.getLogger("fetch_samples")

# US volcanoes used for the FDSN sample (GVP volcano numbers): Hawaii, Alaska, Cascades
# Kilauea, Mauna Loa, Great Sitkin, Shishaldin, Spurr, Mount St. Helens, Rainier
CORE_VOLCANOES = [332010, 332020, 311120, 311360, 313040, 321050, 321030]
FALLBACK_COORDS = {332010: (19.421, -155.287), 332020: (19.475, -155.608),
                   311120: (52.076, -176.13), 311360: (54.756, -163.97),
                   313040: (61.299, -152.251), 321050: (46.2, -122.18),
                   321030: (46.853, -121.76)}
FDSN_SAMPLE_DAYS = 365   # sample window per volcano; the full 2010-> volume is measured with --estimate-volume


def _save(resp, source_id, ext, directory, suffix=""):
    path = save_raw(resp.content, source_id, ext, directory, url=resp.url,
                    headers=dict(resp.headers), suffix=suffix)
    log.info("saved %-60s %8.1f KB", path.relative_to(ROOT), len(resp.content) / 1024)
    return path


def fetch_incremental(session, cfg):
    """HANS alert levels, USGS feed and (optional) weekly report. Returns the parsed weekly items."""
    hans = get_source(cfg, "usgs_hans")
    for name in ("monitored", "elevated"):
        try:
            r = usgs_hans.fetch(session, hans["url"], hans["endpoints"][name])
            _save(r, "usgs_hans", "json", SAMPLES, suffix=name)
            log.info("HANS %s: %d rows", name, len(usgs_hans.parse(r.content)))
        except Exception as exc:
            log.error("HANS %s failed: %s", name, exc)

    items = []
    wk = get_source(cfg, "gvp_weekly_rss")
    try:
        r = get(session, wk["url"])
        _save(r, "gvp_weekly_rss", "xml", SAMPLES)
        items = weekly_report.parse_rss(r.content)
        log.info("weekly report: %d items", len(items))
        r = get(session, wk["alt_url_cap"])
        _save(r, "gvp_weekly_cap", "xml", SAMPLES)
    except Exception as exc:  # log it and continue, a failed source is useful evidence too
        log.error("weekly report failed: %s", exc)

    feed = get_source(cfg, "usgs_feed")
    try:
        r = usgs_feed.fetch(session, feed["url"])
        _save(r, "usgs_feed", "geojson", SAMPLES, suffix="all_week")
    except Exception as exc:
        log.error("USGS feed failed: %s", exc)

    gd = get_source(cfg, "gdacs_vo")
    try:
        r = get(session, gd["url"], params=gd["params"])
        _save(r, "gdacs_vo", "json", SAMPLES)
    except Exception as exc:
        log.warning("GDACS (optional) failed: %s", exc)
    return items


def fetch_historical(session, cfg, weekly_items):
    # HANS list of US volcanoes. Notices have no coordinates, so we need this list to place them.
    hans = get_source(cfg, "usgs_hans")
    try:
        r = usgs_hans.fetch(session, hans["url"], hans["endpoints"]["us_volcanoes"])
        _save(r, "usgs_hans", "json", SAMPLES, suffix="us_volcanoes")
    except Exception as exc:
        log.error("HANS US volcano list failed: %s", exc)

    # GVP volcanoes, already filtered to the US by the CQL_FILTER in sources.yaml
    src = get_source(cfg, "gvp_volcanoes")
    volcanoes = {}
    try:
        r = gvp.fetch_layer(session, src)
        _save(r, "gvp_volcanoes", "geojson", SAMPLES)
        for v in gvp.parse_features(r.content):
            volcanoes[v.get("Volcano_Number")] = v
        log.info("GVP volcanoes: %d", len(volcanoes))
    except Exception as exc:
        log.error("GVP volcanoes failed: %s", exc)

    # GVP eruptions: the full layer goes to data/raw and the US ones to samples.
    # This layer has no Country field, so we filter by the US volcano numbers.
    src = get_source(cfg, "gvp_eruptions")
    try:
        r = gvp.fetch_layer(session, src)
        _save(r, "gvp_eruptions", "geojson", raw_dir(), suffix="full")
    except Exception as exc:
        log.error("GVP eruptions (full) failed: %s", exc)
    if volcanoes:
        try:
            vn_list = ",".join(str(v) for v in sorted(volcanoes))
            params = {**src["params"], "CQL_FILTER": f"Volcano_Number IN ({vn_list})"}
            r = get(session, src["url"], params=params)
            n = len(gvp.parse_features(r.content))
            _save(r, "gvp_eruptions", "geojson", SAMPLES, suffix="us")
            log.info("GVP US eruptions: %d", n)
        except Exception as exc:
            log.error("US eruptions (CQL filter) failed: %s", exc)

    # FDSN earthquakes around a few well-known volcanoes plus the US ones in this week's report
    fd = get_source(cfg, "usgs_fdsn")
    sp = cfg["spatial"]
    now = datetime.now(timezone.utc)
    start = (now - timedelta(days=FDSN_SAMPLE_DAYS)).strftime("%Y-%m-%d")
    end = now.strftime("%Y-%m-%d")
    us_weekly = [i["volcano_number"] for i in weekly_items
                 if i.get("volcano_number") in volcanoes]       # weekly items of US volcanoes only
    vnums = list(dict.fromkeys(CORE_VOLCANOES + us_weekly))
    for vn in vnums:
        v = volcanoes.get(vn)
        lat, lon = (v["Latitude"], v["Longitude"]) if v and v.get("Latitude") is not None \
            else FALLBACK_COORDS.get(vn, (None, None))
        if lat is None:
            log.warning("no coordinates for volcano %s, skipped", vn)
            continue
        try:
            r = usgs_fdsn.query_radius(session, fd, lat, lon, sp["volcano_radius_km"],
                                       start, end, sp["min_magnitude"])
            _save(r, "usgs_fdsn", "csv", SAMPLES, suffix=f"v{vn}")
        except Exception as exc:
            log.error("FDSN for volcano %s failed: %s", vn, exc)
        time.sleep(1.0)  # be polite


def estimate_volume(session, cfg):
    """FDSN count per selected US volcano (2010 -> today) -> workshop_1/samples/usgs_fdsn_counts_*.json."""
    gfiles = sorted(SAMPLES.glob("gvp_volcanoes_*.geojson"))
    if not gfiles:
        log.error("run without --estimate-volume first (needs the GVP volcano list)")
        return
    vols = gvp.parse_features(gfiles[-1].read_bytes())
    min_year = cfg["spatial"]["volcano_selection"]["min_last_eruption_year"]
    sel = [v for v in vols if min_year is None
           or (isinstance(v.get("Last_Eruption_Year"), int) and v["Last_Eruption_Year"] >= min_year)]
    log.info("estimating FDSN volume for %d volcanoes (min last eruption year: %s)", len(sel), min_year)
    fd = get_source(cfg, "usgs_fdsn")
    sp = cfg["spatial"]
    end = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    counts = {}
    for i, v in enumerate(sel, 1):
        try:
            counts[v["Volcano_Number"]] = usgs_fdsn.count(
                session, fd["url"], latitude=v["Latitude"], longitude=v["Longitude"],
                maxradiuskm=sp["volcano_radius_km"], starttime=cfg["project"]["historical_start"],
                endtime=end, minmagnitude=sp["min_magnitude"])
        except Exception as exc:
            log.warning("count failed for %s: %s", v["Volcano_Number"], exc)
        if i % 50 == 0:
            log.info("%d/%d", i, len(sel))
        time.sleep(0.5)
    out = SAMPLES / f"usgs_fdsn_counts_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}.json"
    out.write_text(json.dumps({"radius_km": sp["volcano_radius_km"], "min_magnitude": sp["min_magnitude"],
                               "start": cfg["project"]["historical_start"], "end": end,
                               "counts": counts}, indent=1))
    over = {k: n for k, n in counts.items() if n > usgs_fdsn.MAX_EVENTS_PER_REQUEST}
    log.info("saved %s (total events, with overlaps: %d; volcanoes above the 20,000 limit: %s)",
             out.relative_to(ROOT), sum(counts.values()), over or "none")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--poll", action="store_true", help="only re-poll the incremental sources")
    ap.add_argument("--estimate-volume", action="store_true", help="FDSN count per volcano")
    args = ap.parse_args()
    cfg = load_config()
    session = make_session(user_agent(cfg))
    SAMPLES.mkdir(parents=True, exist_ok=True)
    if args.estimate_volume:
        estimate_volume(session, cfg)
        return
    items = fetch_incremental(session, cfg)
    if not args.poll:
        fetch_historical(session, cfg, items)


if __name__ == "__main__":
    main()
