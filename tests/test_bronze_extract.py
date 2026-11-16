"""Per-source Bronze extraction with a fake HTTP session (no network)."""
import json
from datetime import datetime, timezone
from pathlib import Path

from volcano_pipeline.bronze.extract import extract, gvp_due
from volcano_pipeline.config import load_config

FIX = Path(__file__).parent / "fixtures"


class FakeResponse:
    def __init__(self, content: bytes, url: str):
        self.content, self.url, self.status_code = content, url, 200
        self.text = content.decode("utf-8")

    def raise_for_status(self):
        pass


class FakeSession:
    def __init__(self, content: bytes):
        self.content, self.calls = content, []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append((url, params))
        return FakeResponse(self.content, url)


def test_extract_hans_keeps_bytes_and_parses():
    raw = (FIX / "usgs_hans_monitored.json").read_bytes()
    session = FakeSession(raw)
    ex = extract("usgs_hans", load_config(), session)
    assert ex.content == raw and isinstance(ex.payload, list) and ex.http_status == 200
    assert session.calls[0][0].endswith("/hans-public/api/volcano/getMonitoredVolcanoes")


def test_extract_feed_uses_configured_url():
    fc = json.dumps({"type": "FeatureCollection", "features": []}).encode()
    session = FakeSession(fc)
    ex = extract("usgs_feed", load_config(), session)
    assert ex.url.endswith("all_week.geojson") and ex.payload["features"] == []


def test_extract_gvp_sends_wfs_params():
    session = FakeSession(b'{"type": "FeatureCollection", "features": []}')
    ex = extract("gvp_volcanoes", load_config(), session)
    assert session.calls[0][1]["CQL_FILTER"] == "Country='United States'"
    assert ex.params["typeName"].endswith("Holocene_Volcanoes")


def test_extract_scraper_payload_is_records():
    html = (FIX / "usgs_volcano_updates.html").read_bytes()
    ex = extract("usgs_volcano_updates", load_config(), FakeSession(html))
    assert ex.content == html
    assert {r["record_type"] for r in ex.payload} == {"elevated", "observatory_notice"}


def test_gvp_due_once_per_month(tmp_path):
    nov = datetime(2026, 11, 20, tzinfo=timezone.utc)
    dec = datetime(2026, 12, 1, tzinfo=timezone.utc)
    assert gvp_due(tmp_path, "gvp_volcanoes", nov)
    (tmp_path / "gvp_volcanoes_20261101_000000.json").write_text("{}")
    assert not gvp_due(tmp_path, "gvp_volcanoes", nov)
    assert gvp_due(tmp_path, "gvp_volcanoes", dec)
    assert gvp_due(tmp_path, "gvp_eruptions", nov)


def test_gvp_not_due_after_unchanged_check(tmp_path):
    # an unchanged catalogue writes no Bronze file, but it was still checked this month
    from volcano_pipeline.bronze.extract import mark_checked
    nov = datetime(2026, 11, 20, tzinfo=timezone.utc)
    mark_checked(tmp_path, "gvp_eruptions", nov)
    assert not gvp_due(tmp_path, "gvp_eruptions", nov)
    assert gvp_due(tmp_path, "gvp_eruptions", datetime(2026, 12, 1, tzinfo=timezone.utc))
    assert gvp_due(tmp_path, "gvp_volcanoes", nov)
    assert not list(tmp_path.glob("*.json"))      # the marker is not a Bronze file
