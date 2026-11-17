"""Bronze run outside Airflow (scripts/run_pipeline_local.py), with a fake HTTP session."""
from datetime import datetime, timezone
from pathlib import Path

from volcano_pipeline.bronze.run import run_bronze
from volcano_pipeline.config import load_config
from test_bronze_extract import FakeSession

FIX = Path(__file__).parent / "fixtures"
NOW = datetime(2026, 11, 16, 1, 0, 0, tzinfo=timezone.utc)


def test_run_bronze_writes_then_reports_unchanged(tmp_path):
    session = FakeSession((FIX / "usgs_hans_monitored.json").read_bytes())
    first = run_bronze(["usgs_hans"], load_config(), session, tmp_path, NOW)
    assert first == [{"source_id": "usgs_hans", "status": "written", "file": "usgs_hans_20261116_010000.json"}]
    again = run_bronze(["usgs_hans"], load_config(), session, tmp_path, NOW.replace(hour=2))
    assert again[0]["status"] == "unchanged"


def test_run_bronze_skips_gvp_when_checked_this_month(tmp_path):
    (tmp_path / "gvp_volcanoes_20261101_000000.json").write_text("{}")
    session = FakeSession(b"{}")
    out = run_bronze(["gvp_volcanoes"], load_config(), session, tmp_path, NOW)
    assert out[0]["status"] == "not_due" and session.calls == []


def test_run_bronze_reports_invalid_payload_and_continues(tmp_path):
    session = FakeSession(b"[]")
    out = run_bronze(["usgs_hans"], load_config(), session, tmp_path, NOW)
    assert out[0]["status"] == "invalid" and out[0]["errors"]
    assert not list(tmp_path.glob("*.json"))
