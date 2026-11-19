"""HANS notice texts: search pagination, text parsing, Bronze watermark/backfill and Silver rows.

Fixtures in tests/fixtures/hans_notices/ were recorded on 2026-11-19: six real notices of
different types (HVO daily update, CalVO region-wide weekly update, AVO activity notice and
VONA, AVO weekly update for two volcanoes, HVO monthly update for American Samoa) and the
getUSVolcanoes rows of the volcanoes they mention.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

from volcano_pipeline.bronze import hans_notices as bronze_notices
from volcano_pipeline.bronze.envelope import build_envelope, write_bronze
from volcano_pipeline.bronze.validate import validate_payload
from volcano_pipeline.sources import hans_notices
from volcano_pipeline.silver.transform import hans_volcano_map, notices_from_hans_search

FIX = Path(__file__).parent / "fixtures" / "hans_notices"
PAGE = json.loads((FIX / "search_page.json").read_text(encoding="utf-8"))
NOTICES = PAGE["noticeData"]
US_VOLCANOES = json.loads((FIX / "us_volcanoes.json").read_text(encoding="utf-8"))
CFG = {"sources": [{"source_id": "usgs_hans_notices", "url": "https://hans.test/api", "overlap_hours": 6,
                    "first_window_days": 30, "page_pause_seconds": 0}]}
NOW = datetime(2026, 11, 19, 12, 0, tzinfo=timezone.utc)


class FakeResponse:
    def __init__(self, data: dict):
        self._data, self.content, self.status_code = data, json.dumps(data).encode(), 200

    def json(self):
        return self._data

    def raise_for_status(self):
        pass


class BrokenThenGood:
    """First answer is an empty 200 body (seen in the real backfill), then a valid page."""

    def __init__(self):
        self.calls = 0

    def post(self, url, json=None, timeout=None):
        self.calls += 1
        if self.calls == 1:
            resp = FakeResponse({})
            resp.json = lambda: (_ for _ in ()).throw(ValueError("Expecting value"))
            return resp
        return FakeResponse({"noticeTotal": 1, "noticeData": NOTICES[:1]})


class FakeSession:
    """Answers search/search with `pages` (lists of notices) and records the request bodies."""

    def __init__(self, pages: list[list[dict]]):
        self.pages, self.bodies = pages, []

    def post(self, url, json=None, timeout=None):
        self.bodies.append(json)
        page = self.pages[json["pageIndex"]] if json["pageIndex"] < len(self.pages) else []
        return FakeResponse({"noticeTotal": sum(map(len, self.pages)), "noticeData": page})


def _by_type(cd: str, obs: str) -> dict:
    return next(n for n in NOTICES if n["noticeTypeCd"] == cd and n["obsAbbr"] == obs)


# --------------------------------------------------------------------------- source parsing

def test_search_reads_pages_until_a_short_page():
    session = FakeSession([[NOTICES[0]] * 20, [NOTICES[1]] * 20, [NOTICES[2]] * 3])
    notices, raw, _ = hans_notices.fetch_window(session, "https://hans.test/api", NOW - timedelta(days=1), NOW, 0)
    assert len(notices) == 43 and [b["pageIndex"] for b in session.bodies] == [0, 1, 2]
    assert session.bodies[0]["obsAbbr"] == ""          # "all" matches nothing in the real API
    assert session.bodies[0]["startUnixtime"] == int((NOW - timedelta(days=1)).timestamp())


def test_empty_body_is_retried_then_read():
    session = BrokenThenGood()
    notices, _, _ = hans_notices.fetch_window(session, "https://hans.test/api", NOW - timedelta(days=1), NOW, 0, 0)
    assert session.calls == 2 and len(notices) == 1


def test_daily_update_summary_level_and_footer():
    parsed = hans_notices.parse_notice(_by_type("DU", "hvo")["noticeHtml"])
    assert parsed["summary_raw"].startswith("The Halema'uma'u eruption")
    assert (parsed["alert_level"], parsed["color_code"]) == ("ADVISORY", "YELLOW")
    assert parsed["vnums_in_text"] == [332010]
    body = parsed["body_raw"].lower()
    assert "askhvo@usgs.gov" not in body and "subscribe to these messages" not in body
    assert "https://" not in body


def test_vona_summary_is_the_remarks_line_and_has_no_alert_level():
    parsed = hans_notices.parse_notice(_by_type("VV", "avo")["noticeHtml"])
    assert parsed["summary_raw"].startswith("NO ERUPTION")
    assert parsed["alert_level"] is None and parsed["color_code"] == "GREEN"


def test_highest_level_wins_when_a_notice_covers_several_volcanoes():
    text = "Volcano Alert Level: NORMAL ... Volcano Alert Level: WATCH ... Volcano Alert Level: ADVISORY"
    assert hans_notices.highest(hans_notices._LEVEL, text, hans_notices.ALERT_LEVELS) == "WATCH"


# --------------------------------------------------------------------------- Bronze

def test_first_incremental_window_then_watermark_with_overlap(tmp_path):
    start, end = bronze_notices.incremental_window(CFG, tmp_path, NOW)
    assert (start, end) == (NOW - timedelta(days=30), NOW)
    newest = bronze_notices.advance_watermark(tmp_path, NOTICES)
    assert newest == max(hans_notices.sent_time(n) for n in NOTICES)
    start, _ = bronze_notices.incremental_window(CFG, tmp_path, NOW)
    assert start == newest - timedelta(hours=6)


def test_watermark_never_moves_backwards(tmp_path):
    newest = bronze_notices.advance_watermark(tmp_path, NOTICES)
    older = [n for n in NOTICES if hans_notices.sent_time(n) < newest][:1]
    assert bronze_notices.advance_watermark(tmp_path, older) == newest
    assert bronze_notices.advance_watermark(tmp_path, []) == newest


def test_empty_window_writes_no_bronze_file(tmp_path):
    env = build_envelope("usgs_hans_notices", "u", {}, 200, b"{}", [], NOW)
    assert validate_payload("usgs_hans_notices", []) == []
    assert write_bronze(env, tmp_path) is None and not list(tmp_path.glob("*.json"))


def test_month_windows_cover_the_range_without_gaps():
    windows = bronze_notices.month_windows(datetime(2025, 11, 15, tzinfo=timezone.utc), NOW)
    assert windows[0] == (datetime(2025, 11, 15, tzinfo=timezone.utc), datetime(2025, 12, 1, tzinfo=timezone.utc))
    assert windows[-1][1] == NOW and len(windows) == 13
    assert all(a[1] == b[0] for a, b in zip(windows, windows[1:]))


def test_backfill_skips_months_already_loaded(tmp_path):
    start = datetime(2026, 9, 1, tzinfo=timezone.utc)
    end = datetime(2026, 11, 1, tzinfo=timezone.utc)
    first = bronze_notices.run_backfill(CFG, FakeSession([NOTICES]), tmp_path, start, end)
    assert first["months"] == 2 and first["files"] == 2
    session = FakeSession([NOTICES])
    again = bronze_notices.run_backfill(CFG, session, tmp_path, start, end)
    assert again["months"] == 0 and session.bodies == []
    assert bronze_notices.read_watermark(tmp_path) is not None


# --------------------------------------------------------------------------- Silver

def _silver(notices=NOTICES):
    env = build_envelope("usgs_hans_notices", "u", {}, 200, b"x", notices, NOW)
    return notices_from_hans_search(env, {"hans_volcanoes": hans_volcano_map({"payload": US_VOLCANOES})})


def test_silver_rows_map_codes_to_vnum_and_region():
    topic, df, rejects = _silver()
    assert topic == "notices" and len(df) == len(NOTICES) and not rejects
    du = df[df["notice_type"] == "DU"].iloc[0]
    assert du["vnum"] == 332010 and du["volcano_name"] == "Kilauea" and "Hawaii" in du["region"]
    two = df[df["volcano_codes"] == "ak111,ak124"].iloc[0]
    assert list(two["vnums"]) == [311120, 313020]
    assert df["record_id"].str.startswith("usgs_hans_notice:DOI-USGS-").all()


def test_van_and_vona_with_the_same_identifier_are_two_records():
    van, vona = _by_type("VAN", "avo"), _by_type("VV", "avo")
    twin = dict(vona, noticeIdentifier=van["noticeIdentifier"])     # as in 708 real cases
    _, df, _ = _silver([van, twin])
    assert df["record_id"].nunique() == 2
    assert set(df["record_id"].str.rsplit(":", n=1).str[1]) == {"VAN", "VV"}


def test_older_format_summary_and_header_lines_removed():
    html = ("<p>HAWAIIAN VOLCANO OBSERVATORY DAILY UPDATE</p><p>U.S. Geological Survey</p>"
            "<p>Sunday, June 3, 2012, 7:31 AM HST (Sunday, June 3, 2012, 17:31 UTC)</p>"
            "<p>(VNUM #332010)</p><p>19°25'16\" N 155°17'13\" W,</p><p>Summit Elevation 4091 ft (1247 m)</p>"
            "<p>Current Volcano Alert Level: WATCH</p><p>Current Aviation Color Code: ORANGE</p>"
            "<p>Activity Summary for past 24 hours:</p><p>DI inflation and the rise of the lava lake began.</p>"
            "<p>Background:</p><p>The summit lava lake is deep within a vent, a text repeated every day.</p>"
            "<p>Past 24 hours at the east rift:</p><p>Flows advanced on the coastal plain.</p>")
    p = hans_notices.parse_notice(html)
    assert p["summary_raw"] == "DI inflation and the rise of the lava lake began."
    assert p["alert_level"] == "WATCH" and p["vnums_in_text"] == [332010]
    assert "Summit Elevation" not in p["body_raw"] and "repeated every day" not in p["body_raw"]
    assert "coastal plain" in p["body_raw"]


def test_region_wide_notice_has_no_volcano():
    _, df, _ = _silver()
    calvo = df[(df["notice_type"] == "WU") & (df["observatory"] == "calvo")].iloc[0]
    assert calvo["is_region_wide"] and pd.isna(calvo["vnum"])


def test_text_features_are_filled():
    _, df, _ = _silver()
    assert (df["token_count"] > 0).all() and (df["text_length"] >= df["summary_length"]).all()
    assert df.loc[df["notice_type"] == "DU", "kw_lava"].all()


def test_reference_volcano_list_is_processed_without_quarantine(tmp_path):
    from volcano_pipeline.silver.run import process_bronze_files

    bronze = tmp_path / "bronze"
    write_bronze(build_envelope("usgs_hans_volcanoes", "u", {}, 200, b"v", US_VOLCANOES, NOW), bronze)
    out = process_bronze_files(bronze, tmp_path / "silver", when=NOW)
    assert out["rejected"] == 0 and out["written"] == {} and len(out["processed"]) == 1


def test_notice_without_time_goes_to_quarantine():
    broken = dict(NOTICES[0], sentUnixtime=None)
    _, df, rejects = _silver([broken])
    assert df.empty and rejects[0]["reason"] == "missing_id_or_time"
    assert "noticeHtml" not in rejects[0]["payload"]


@pytest.mark.parametrize("payload, ok", [([], True), ([{"noticeIdentifier": "x"}], False), ({}, False)])
def test_validation(payload, ok):
    assert (validate_payload("usgs_hans_notices", payload) == []) is ok
