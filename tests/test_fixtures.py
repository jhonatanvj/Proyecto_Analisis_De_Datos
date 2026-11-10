"""Tests on real responses recorded on 2026-11-10 (tests/fixtures, no network)."""
from collections import Counter
from pathlib import Path

from volcano_pipeline.normalization.canonical import map_hans_notice
from volcano_pipeline.sources import usgs_hans, weekly_report

FIX = Path(__file__).parent / "fixtures"


def test_recorded_hans_snapshot():
    rows = usgs_hans.parse((FIX / "usgs_hans_monitored.json").read_bytes())
    assert len(rows) == 69
    reasons = Counter(usgs_hans.reject_reason(r) for r in rows)
    assert reasons["region_level_notice"] == 2           # "Alaskan Volcanoes", "Cascade Range"
    assert reasons["no_notice_issued"] == 13             # sent_unixtime = 0
    valid = [r for r in rows if usgs_hans.reject_reason(r) is None]
    events = [map_hans_notice(r, {}) for r in valid]
    assert len({e["event_id"] for e in events}) == len(events)   # duplicate key is unique
    sitkin = next(e for e in events if e["event_id"].startswith("usgs_hans:311120:"))
    assert sitkin["severity"] == 3                        # WATCH


def test_recorded_weekly_rss():
    items = weekly_report.parse_rss((FIX / "gvp_weekly_rss.xml").read_bytes())
    assert len(items) == 27
    assert all(i["title_parse_ok"] and i["volcano_number"] for i in items)
    assert Counter(i["status"] for i in items) == {"ONGOING": 23, "NEW": 4}
    us = {i["volcano_number"] for i in items if i["country"] == "United States"}
    assert us == {311120, 332010}                        # Great Sitkin, Kilauea
