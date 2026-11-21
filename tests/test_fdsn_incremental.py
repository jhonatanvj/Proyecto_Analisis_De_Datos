"""Incremental FDSN (updatedafter = watermark): window, one query per region, dedup, watermark."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest

from volcano_pipeline.bronze import fdsn_incremental as inc
from volcano_pipeline.bronze.envelope import build_envelope, write_bronze

FEED = json.loads((Path(__file__).parent / "fixtures" / "bronze" / "usgs_feed_20261116_010000.json")
                  .read_text(encoding="utf-8"))["payload"]
NOW = datetime(2026, 11, 19, 12, 0, tzinfo=timezone.utc)
CFG = {"spatial": {"min_magnitude": 1.0}, "sources": [{
    "source_id": "usgs_fdsn", "url": "https://fdsn.test/query",
    "incremental": {"event_start": "2010-01-01", "overlap_minutes": 60, "first_window_days": 7, "max_events": 50,
                    "regions": {"hawaii": [18, 23, -161, -154], "alaska": [50, 67, -180, -129]}}}]}


class FakeResponse:
    def __init__(self, data):
        self.content, self.status_code = json.dumps(data).encode(), 200

    def raise_for_status(self):
        pass


class FakeSession:
    def __init__(self, per_region: list[list[dict]]):
        self.answers, self.params = list(per_region), []

    def get(self, url, params=None, headers=None, timeout=None):
        self.params.append(params)
        return FakeResponse({"type": "FeatureCollection", "features": self.answers.pop(0)})


def test_first_run_reads_a_week_then_starts_before_the_watermark(tmp_path):
    assert inc.updated_after(CFG, tmp_path, NOW) == NOW - timedelta(days=7)
    newest = inc.advance_watermark(tmp_path, FEED)
    assert newest == datetime.fromtimestamp(max(f["properties"]["updated"] for f in FEED["features"]) / 1000,
                                            tz=timezone.utc)
    assert inc.updated_after(CFG, tmp_path, NOW) == newest - timedelta(minutes=60)


def test_one_query_per_region_with_updatedafter_and_old_event_start(tmp_path):
    feats = FEED["features"]
    session = FakeSession([feats[:3], feats[2:5]])          # feats[2] comes back from both regions
    ex = inc.extract_incremental(CFG, session, tmp_path, NOW)
    assert len(session.params) == 2
    p = session.params[0]
    assert p["updatedafter"] == (NOW - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%S")
    assert p["starttime"] == "2010-01-01" and p["minmagnitude"] == 1.0 and p["format"] == "geojson"
    assert {f["id"] for f in ex.payload["features"]} == {f["id"] for f in feats[:5]}
    assert len(ex.payload["features"]) == 5


def test_full_answer_means_the_window_is_too_wide(tmp_path):
    session = FakeSession([FEED["features"][:1] * 50, []])
    with pytest.raises(RuntimeError, match="too wide"):
        inc.extract_incremental(CFG, session, tmp_path, NOW)


def test_watermark_only_moves_forward(tmp_path):
    newest = inc.advance_watermark(tmp_path, FEED)
    old = {"features": [{"properties": {"updated": 1_000_000}}]}
    assert inc.advance_watermark(tmp_path, old) == newest
    assert inc.advance_watermark(tmp_path, {"features": []}) == newest


def test_no_new_event_writes_no_bronze_file(tmp_path):
    env = build_envelope("usgs_fdsn", "u", {}, 200, b"{}", {"type": "FeatureCollection", "features": []}, NOW)
    assert write_bronze(env, tmp_path) is None
