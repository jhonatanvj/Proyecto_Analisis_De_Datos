"""Unit tests for the Workshop #1 helpers (no network)."""
from datetime import datetime, timezone

from volcano_pipeline.normalization.canonical import (content_hash, gvp_eruption_date,
                                                      map_usgs_feature)
from volcano_pipeline.normalization.geo import haversine_km, normalize_longitude, valid_coordinates
from volcano_pipeline.sources.usgs_feed import new_or_changed
from volcano_pipeline.sources.weekly_report import parse_rss, parse_title, parse_week_range
from volcano_pipeline.storage.raw_landing import landing_name, save_raw

RSS = b"""<?xml version="1.0"?>
<rss version="2.0" xmlns:georss="http://www.georss.org/georss"><channel>
<item><title>Etna (Italy) - Report for 29 October-4 November 2025 - NEW</title>
<link>https://volcano.si.edu/showreport.cfm?wvar=GVP.WVAR20251029-211060</link>
<description>Test</description><pubDate>Wed, 05 Nov 2025 23:00:00 GMT</pubDate>
<georss:point>37.748 14.999</georss:point></item>
</channel></rss>"""


def test_landing_name_format():
    when = datetime(2026, 11, 9, 21, 5, 7, tzinfo=timezone.utc)
    assert landing_name("usgs_feed", "geojson", when) == "usgs_feed_20261109_210507.geojson"


def test_save_raw_keeps_bytes(tmp_path):
    p = save_raw(b"abc", "src", "txt", tmp_path, url="u", headers={"ETag": "x", "Server": "y"})
    assert p.read_bytes() == b"abc"
    meta = p.with_suffix(".txt.meta.json").read_text()
    assert '"ETag"' in meta and "Server" not in meta


def test_haversine_known_distance():
    # Bogota -> Medellin ~ 240 km
    assert 230 < haversine_km(4.711, -74.072, 6.244, -75.581) < 250


def test_coordinates():
    assert valid_coordinates(4.9, -75.3)
    assert not valid_coordinates(95, 0)
    assert not valid_coordinates(None, 0)
    assert normalize_longitude(190) == -170


def test_week_range_crossing_year():
    assert parse_week_range("31 December 2025-6 January 2026") == ("2025-12-31", "2026-01-06")
    assert parse_week_range("29 October-4 November 2025") == ("2025-10-29", "2025-11-04")
    assert parse_week_range("5 November-11 November 2025")[0] == "2025-11-05"


def test_title_status():
    t = parse_title("Ruiz, Nevado del (Colombia) - Report for 29 October-4 November 2025")
    assert t["volcano_name"] == "Ruiz, Nevado del" and t["status"] == "ONGOING"


def test_parse_rss():
    items = parse_rss(RSS)
    assert items[0]["volcano_number"] == 211060
    assert items[0]["status"] == "NEW"
    assert items[0]["latitude"] == 37.748


def test_gvp_dates():
    assert gvp_eruption_date(2021, 9, 19) == ("2021-09-19T00:00:00+00:00", "day")
    assert gvp_eruption_date(1985, 0, 0)[1] == "year"
    assert gvp_eruption_date(-500, None, None) == (None, "bce")


def test_usgs_mapping_and_watermark():
    f = {"id": "us7000abcd", "geometry": {"coordinates": [-75.3, 4.9, 3.2]},
         "properties": {"time": 1762700000000, "updated": 1762700100000, "mag": 3.1,
                        "magType": "ml", "url": "https://x"}}
    e = map_usgs_feature(f)
    assert e["event_id"] == "usgs:us7000abcd" and e["timestamp"].endswith("+00:00")
    assert new_or_changed([f], 1762700000000) == [f]
    assert new_or_changed([f], 1762700100000) == []
    assert content_hash({"a": 1, "b": 2}) == content_hash({"b": 2, "a": 1})
