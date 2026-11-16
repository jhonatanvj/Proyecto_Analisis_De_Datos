"""Bronze JSON envelope, payload validation and idempotent writer."""
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from volcano_pipeline.bronze.envelope import build_envelope, latest_bronze, write_bronze
from volcano_pipeline.bronze.validate import count_records, validate_payload
from volcano_pipeline.sources.volcano_updates import parse_volcano_updates

FIX = Path(__file__).parent / "fixtures"
T1 = datetime(2026, 11, 16, 1, 0, 0, tzinfo=timezone.utc)
T2 = datetime(2026, 11, 16, 2, 0, 0, tzinfo=timezone.utc)


def test_envelope_fields():
    env = build_envelope("usgs_hans", "https://x", {"a": 1}, 200, b"[1, 2]", [1, 2], T1)
    assert env["retrieved_utc"] == "2026-11-16T01:00:00+00:00"
    assert env["content_sha256"] == hashlib.sha256(b"[1, 2]").hexdigest()
    assert env["record_count"] == 2 and env["payload"] == [1, 2]


def test_write_bronze_skips_unchanged(tmp_path):
    env = build_envelope("usgs_hans", "u", {}, 200, b"[1]", [], T1)
    assert write_bronze(env, tmp_path).name == "usgs_hans_20261116_010000.json"
    env2 = build_envelope("usgs_hans", "u", {}, 200, b"[1]", [], T2)
    assert write_bronze(env2, tmp_path) is None
    assert len(list(tmp_path.glob("*.json"))) == 1


def test_write_bronze_writes_changed_content(tmp_path):
    write_bronze(build_envelope("usgs_hans", "u", {}, 200, b"[1]", [], T1), tmp_path)
    p = write_bronze(build_envelope("usgs_hans", "u", {}, 200, b"[2]", [], T2), tmp_path)
    assert p is not None and json.loads(p.read_text(encoding="utf-8"))["content_sha256"]


def test_latest_bronze_ignores_prefix_collisions(tmp_path):
    (tmp_path / "usgs_feed_20261116_010000.json").write_text("{}")
    (tmp_path / "usgs_feed_day_20261116_020000.json").write_text("{}")
    assert latest_bronze(tmp_path, "usgs_feed").name == "usgs_feed_20261116_010000.json"
    assert latest_bronze(tmp_path, "usgs_hans") is None


def test_validate_scrape_layout_change():
    assert validate_payload("usgs_volcano_updates", []) != []
    recs = parse_volcano_updates((FIX / "usgs_volcano_updates.html").read_text(encoding="utf-8"))
    assert validate_payload("usgs_volcano_updates", recs) == []
    assert count_records("usgs_volcano_updates", recs) == len(recs)


def test_validate_hans():
    assert validate_payload("usgs_hans", [{"vnum": "1"}]) != []
    rows = json.loads((FIX / "usgs_hans_monitored.json").read_text(encoding="utf-8"))
    assert validate_payload("usgs_hans", rows) == []


def test_validate_geojson_sources():
    fc = {"type": "FeatureCollection", "features": [{"id": "x", "properties": {}, "geometry": None}]}
    assert validate_payload("usgs_feed", fc) == []
    assert count_records("gvp_volcanoes", fc) == 1
    assert validate_payload("usgs_feed", {"type": "FeatureCollection"}) != []
    assert validate_payload("gvp_eruptions", [1]) != []


def test_validate_unknown_source():
    assert validate_payload("nope", {}) == ["unknown source_id 'nope'"]
