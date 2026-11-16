"""Scraper for the USGS Volcano Updates page, tested on a trimmed copy of the real page (2026-11-16)."""
from pathlib import Path

from volcano_pipeline.sources.volcano_updates import parse_volcano_updates

FIXTURE = (Path(__file__).parent / "fixtures" / "usgs_volcano_updates.html").read_text(encoding="utf-8")


def test_parse_elevated_blocks():
    recs = parse_volcano_updates(FIXTURE)
    el = [r for r in recs if r["record_type"] == "elevated"]
    assert len(el) == 2
    assert el[0]["volcano_name"] == "Kilauea"
    assert el[0]["observatory"] == "hvo"
    assert (el[0]["alert_level"], el[0]["color_code"]) == ("ADVISORY", "YELLOW")
    assert el[0]["notice_url"].startswith("https://volcanoes.usgs.gov/hans2/view/notice/")
    assert el[0]["as_of_raw"] == "2026-11-16 17:26:55 UTC"
    assert "Kīlauea" in el[0]["text_html"]


def test_parse_observatory_notice():
    avo = next(r for r in parse_volcano_updates(FIXTURE)
               if r["observatory"] == "avo" and r["record_type"] == "observatory_notice")
    assert avo["vnum"] == "312170" and avo["volcano_name"] == "Katmai"
    assert avo["notice_number"] == "2026/A844" and avo["alert_level"] == "NORMAL"
    assert avo["color_code"] == "GREEN"
    assert "<p>" in avo["text_html"]


def test_parse_daily_update_without_notice_number():
    hvo = next(r for r in parse_volcano_updates(FIXTURE)
               if r["observatory"] == "hvo" and r["record_type"] == "observatory_notice")
    assert hvo["vnum"] == "332010" and hvo["volcano_name"] == "KILAUEA"
    assert hvo["notice_number"] is None
    assert hvo["issued_raw"] == "Monday, November 16, 2026, 17:26 UTC"


def test_parse_empty_page_returns_nothing():
    assert parse_volcano_updates("<html><body>maintenance</body></html>") == []
