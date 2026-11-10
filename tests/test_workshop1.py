"""Unit tests for the Workshop #1 helpers (no network)."""
from datetime import datetime, timezone

from volcano_pipeline.normalization.canonical import (content_hash, gvp_eruption_date,
                                                      map_gvp_eruption, map_hans_notice,
                                                      map_usgs_feature)
from volcano_pipeline.normalization.geo import haversine_km, normalize_longitude, valid_coordinates
from volcano_pipeline.sources.usgs_feed import new_or_changed
from volcano_pipeline.sources.usgs_hans import new_notices, reject_reason, valid_vnum
from volcano_pipeline.sources.weekly_report import parse_rss, parse_title, parse_week_range
from volcano_pipeline.storage.raw_landing import landing_name, save_raw

RSS = b"""<?xml version="1.0"?>
<rss version="2.0" xmlns:georss="http://www.georss.org/georss"><channel>
<item><title>Etna (Italy) - Report for 29 October-4 November 2025 - NEW</title>
<link>https://volcano.si.edu/showreport.cfm?wvar=GVP.WVAR20251029-211060</link>
<description>Test</description><pubDate>Wed, 05 Nov 2025 23:00:00 GMT</pubDate>
<georss:point>37.748 14.999</georss:point></item>
</channel></rss>"""


# Real format observed on 2026-11-09: ISO-8859-1, generic <link>, vnum only in <guid>,
# status as a "- New/Continuing Eruptive Activity" suffix.
RSS_2026 = """<?xml version="1.0" encoding="ISO-8859-1"?>
<rss version="2.0" xmlns:georss="http://www.georss.org/georss"><channel>
<item><title>Great Sitkin (United States) - Report for 8 October-14 October 2026 - Continuing Eruptive Activity</title>
<description>&lt;p&gt;Lava effusion continued in Great Sitkin\xbfs summit crater.&lt;/p&gt;</description>
<link>https://volcano.si.edu/reports_weekly.cfm</link>
<guid isPermaLink="true">https://volcano.si.edu/reports_weekly.cfm#vn_311120</guid>
<pubDate>Thu, 15 Oct 2026 03:11:08 -0400</pubDate>
<georss:point>52.0760 -176.1300</georss:point></item>
<item><title>Krakatau (Indonesia) - Report for 8 October-14 October 2026 - New Eruptive Activity</title>
<description>x</description><link>https://volcano.si.edu/reports_weekly.cfm</link>
<guid isPermaLink="true">https://volcano.si.edu/reports_weekly.cfm#vn_262000</guid>
<pubDate>Thu, 15 Oct 2026 03:11:08 -0400</pubDate></item>
</channel></rss>""".encode("latin-1")


def test_parse_rss_2026_format():
    a, b = parse_rss(RSS_2026)
    assert a["volcano_number"] == 311120 and a["country"] == "United States"
    assert a["status"] == "ONGOING" and b["status"] == "NEW"
    assert (a["week_start"], a["week_end"]) == ("2026-10-08", "2026-10-14")
    assert a["pub_date_utc"] == "2026-10-15T07:11:08+00:00"
    assert b["latitude"] is None


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


def test_gvp_eruption_uses_geometry_coordinates():
    # The eruptions layer has no Latitude/Longitude properties (verified 2026-11-09)
    props = {"Volcano_Number": 342110, "Eruption_Number": 10750, "ExplosivityIndexMax": 2,
             "StartDateYear": 1690, "StartDateMonth": 0, "StartDateDay": 0,
             "_lat": 14.382, "_lon": -90.601}
    e = map_gvp_eruption(props)
    assert (e["latitude"], e["longitude"]) == (14.382, -90.601)
    assert e["timestamp"] == "1690-01-01T00:00:00+00:00" and e["severity"] == 2


HANS_ROW = {"volcano_name": "Great Sitkin", "vnum": "311120", "notice_type_cd": "DU",
            "notice_identifier": "DOI-USGS-AVO-2026-11-09T20:51:49+00:00",
            "sent_utc": "2026-11-09 21:09:11", "sent_unixtime": 1794258551,
            "color_code": "ORANGE", "alert_level": "WATCH",
            "notice_url": "https://volcanoes.usgs.gov/hans-public/notice/DOI-USGS-AVO-2026-11-09T20:51:49+00:00"}


def test_hans_mapping():
    e = map_hans_notice(HANS_ROW, {311120: (52.0765, -176.1109)})
    assert e["event_id"] == "usgs_hans:311120:DOI-USGS-AVO-2026-11-09T20:51:49+00:00"
    assert e["event_type"] == "alert_notice" and e["severity"] == 3
    assert e["timestamp"] == "2026-11-09T21:09:11+00:00"      # matches sent_utc
    assert e["latitude"] == 52.0765


def test_hans_vnum_and_new_notices():
    assert valid_vnum("311120") and not valid_vnum("0") and not valid_vnum("999999")
    assert not valid_vnum(None)
    seen = {("311120", HANS_ROW["notice_identifier"])}
    assert new_notices([HANS_ROW], seen) == []
    assert new_notices([{**HANS_ROW, "notice_identifier": "X"}], seen)[0]["notice_identifier"] == "X"


def test_hans_reject_reasons():
    assert reject_reason(HANS_ROW) is None
    assert reject_reason({**HANS_ROW, "vnum": None}) == "region_level_notice"
    assert reject_reason({**HANS_ROW, "vnum": "999999"}) == "invalid_vnum"
    assert reject_reason({**HANS_ROW, "notice_identifier": None, "sent_unixtime": 0}) == "no_notice_issued"
    assert reject_reason({**HANS_ROW, "alert_level": "UNASSIGNED"}) == "unknown_alert_level"


def test_usgs_mapping_and_watermark():
    f = {"id": "us7000abcd", "geometry": {"coordinates": [-75.3, 4.9, 3.2]},
         "properties": {"time": 1762700000000, "updated": 1762700100000, "mag": 3.1,
                        "magType": "ml", "url": "https://x"}}
    e = map_usgs_feature(f)
    assert e["event_id"] == "usgs:us7000abcd" and e["timestamp"].endswith("+00:00")
    assert new_or_changed([f], 1762700000000) == [f]
    assert new_or_changed([f], 1762700100000) == []
    assert content_hash({"a": 1, "b": 2}) == content_hash({"b": 2, "a": 1})
