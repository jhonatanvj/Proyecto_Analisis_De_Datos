"""FDSN historical backfill: partitioning under the 20,000-event limit, request params, unique Bronze names."""
import json
from datetime import datetime, timedelta, timezone

from volcano_pipeline.bronze.envelope import build_envelope
from volcano_pipeline.bronze.fdsn_backfill import (fetch_partition, plan_partitions, volcano_partitions,
                                                   write_bronze_unique)
from volcano_pipeline.bronze.validate import validate_payload
from volcano_pipeline.config import load_config
from test_bronze_extract import FakeSession

UTC = timezone.utc
START, END = datetime(2010, 1, 1, tzinfo=UTC), datetime(2026, 11, 16, tzinfo=UTC)


def counter(events):
    calls = []

    def count(a, b):
        calls.append((a, b))
        return sum(1 for e in events if a <= e <= b)
    count.calls = calls
    return count


def test_small_volcano_is_one_partition():
    count = counter([START + timedelta(days=d) for d in range(100)])
    assert plan_partitions(count, START, END, limit=20_000) == [(START, END)]


def test_quiet_volcano_has_no_partition():
    assert plan_partitions(counter([]), START, END) == []


def test_busy_volcano_is_split_until_each_part_fits():
    # 2018: a burst of events (like Kilauea), the rest of the years quiet
    burst = [datetime(2018, 5, 1, tzinfo=UTC) + timedelta(minutes=m) for m in range(50)]
    quiet = [START + timedelta(days=30 * k) for k in range(20)]
    count = counter(burst + quiet)
    parts = plan_partitions(count, START, END, limit=20)
    assert all(count(a, b) <= 20 or (b - a) <= timedelta(days=1) for a, b in parts)
    covered = {e for a, b in parts for e in burst + quiet if a <= e <= b}
    assert covered == set(burst + quiet)                    # no event lost
    assert parts == sorted(parts) and parts[0][0] == START
    assert all(count(a, b) > 0 for a, b in parts)           # empty ranges are not requested


def test_volcano_partitions_uses_count_endpoint():
    session = FakeSession(b"12\n")
    parts = volcano_partitions({332010: (19.421, -155.287)}, load_config(), session, START, END)
    assert parts == [{"vnum": 332010, "lat": 19.421, "lon": -155.287,
                      "start": START.isoformat(), "end": END.isoformat(), "expected": 12}]
    url, params = session.calls[0]
    assert url.endswith("/fdsnws/event/1/count")
    assert params["maxradiuskm"] == 30 and params["minmagnitude"] == 1.0


def test_fetch_partition_requests_geojson():
    fc = {"type": "FeatureCollection", "features": [{"id": "hv1", "properties": {}, "geometry": None}]}
    session = FakeSession(json.dumps(fc).encode())
    part = {"vnum": 332010, "lat": 19.421, "lon": -155.287,
            "start": START.isoformat(), "end": END.isoformat(), "expected": 1}
    ex = fetch_partition(part, load_config(), session)
    params = session.calls[0][1]
    assert params["format"] == "geojson" and params["orderby"] == "time-asc"
    assert params["latitude"] == 19.421 and params["maxradiuskm"] == 30
    assert ex.source_id == "usgs_fdsn" and ex.payload["features"][0]["id"] == "hv1"
    assert ex.params["vnum"] == 332010
    assert validate_payload("usgs_fdsn", ex.payload) == []


def test_write_bronze_unique_never_overwrites(tmp_path):
    when = datetime(2026, 11, 16, 1, 0, 0, tzinfo=UTC)
    a = write_bronze_unique(build_envelope("usgs_fdsn", "u", {}, 200, b"a", {"features": [1]}, when), tmp_path)
    b = write_bronze_unique(build_envelope("usgs_fdsn", "u", {}, 200, b"b", {"features": [2]}, when), tmp_path)
    assert a.name == "usgs_fdsn_20261116_010000.json"
    assert b.name == "usgs_fdsn_20261116_010001.json"
    assert json.loads(a.read_text(encoding="utf-8"))["payload"]["features"] == [1]
    assert not list(tmp_path.glob("*.part"))


def test_write_bronze_unique_skips_empty_partition(tmp_path):
    when = datetime(2026, 11, 16, 1, 0, 0, tzinfo=UTC)
    env = build_envelope("usgs_fdsn", "u", {}, 200, b"{}", {"features": []}, when)
    assert write_bronze_unique(env, tmp_path) is None and not list(tmp_path.iterdir())


def test_volcano_coords_from_latest_gvp_bronze(tmp_path):
    import shutil
    from pathlib import Path
    from volcano_pipeline.bronze.fdsn_backfill import volcano_coords_from_bronze
    assert volcano_coords_from_bronze(tmp_path) == {}
    src = Path(__file__).parent / "fixtures" / "bronze" / "gvp_volcanoes_20261116_000000.json"
    shutil.copy(src, tmp_path / src.name)
    coords = volcano_coords_from_bronze(tmp_path)
    assert coords[332010] == (19.421, -155.287) and len(coords) == 4
    assert set(volcano_coords_from_bronze(tmp_path, only=[332010, 999])) == {332010}


class RoutingSession(FakeSession):
    """/count answers a number, /query a FeatureCollection with that many events."""

    def __init__(self, n):
        super().__init__(b"")
        self.n = n

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append((url, params))
        from test_bronze_extract import FakeResponse
        if url.endswith("/count"):
            return FakeResponse(f"{self.n}\n".encode(), url)
        feats = [{"type": "Feature", "id": f"hv{i}", "properties": {"type": "earthquake"},
                  "geometry": {"type": "Point", "coordinates": [-155.28, 19.41, 1.0]}} for i in range(self.n)]
        return FakeResponse(json.dumps({"type": "FeatureCollection", "features": feats}).encode(), url)


def test_run_fdsn_backfill_writes_one_file_per_partition(tmp_path):
    from volcano_pipeline.bronze.fdsn_backfill import run_fdsn_backfill
    out = run_fdsn_backfill({332010: (19.421, -155.287), 311120: (52.076, -176.13)}, load_config(),
                            RoutingSession(3), tmp_path, START, END)
    assert out == {"partitions": 2, "events": 6, "files": 2}
    assert len(list(tmp_path.glob("usgs_fdsn_*.json"))) == 2


def test_parse_day_accepts_blank_words_and_dates():
    import pytest
    from datetime import date
    from volcano_pipeline.bronze.fdsn_backfill import parse_day
    default = date(2026, 11, 18)
    for value in (None, "", "  ", "empty", "today", "TODAY", "none"):
        assert parse_day(value, default) == datetime(2026, 11, 18, tzinfo=UTC)
    assert parse_day(" 2010-01-01 ", default) == datetime(2010, 1, 1, tzinfo=UTC)
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        parse_day("01/02/2010", default)
