"""Volcano Activity Indicator in the Gold layer (PySpark), on a small synthetic Silver folder.

Runs where Spark can start (Java 8/11/17), i.e. inside the Airflow image:
    docker compose exec airflow-scheduler python -m pytest -q tests/test_gold_indicator.py

The run time is Monday 2026-11-23 03:00 UTC and the history starts 2026-08-31, so the baseline is
the 12 complete weeks 2026-08-31 ... 2026-11-16 and the seismic window is the 7 days before the run.
"""
from __future__ import annotations

import math
from datetime import date, datetime, timezone

import pandas as pd
import pytest

from test_gold import spark  # noqa: F401
from volcano_pipeline.config import load_config
from volcano_pipeline.gold.vai_rules import combine, level

WHEN = datetime(2026, 11, 23, 3, 0, tzinfo=timezone.utc)
CFG = load_config()["indicator"] | {"history_start": "2026-08-31"}
UTC = "UTC"


def _ts(text):
    return pd.Timestamp(text, tz=UTC)


def _write(folder, topic, rows):
    df = pd.DataFrame(rows)
    df["record_id"] = [f"{topic}:{i}" for i in range(len(df))]
    df["processed_utc"] = _ts("2026-11-23 02:00")
    (folder / topic).mkdir(parents=True)
    # microseconds, like Silver: Spark 3.5 cannot read nanosecond Parquet timestamps
    df.to_parquet(folder / topic / f"{topic}_20261123_020000.parquet", index=False,
                  coerce_timestamps="us", allow_truncated_timestamps=True)


@pytest.fixture
def silver(spark, tmp_path):  # noqa: F811
    """A: WATCH alert + earthquakes; B: continuing eruption only; C: nothing usable; D: old eruption."""
    from volcano_pipeline.gold.silver_reader import read_silver

    folder = tmp_path / "datalake_silver"
    _write(folder, "volcanoes", [
        {"vnum": 1001, "name": "Alpha", "latitude": 52.0, "longitude": -176.0,
         "subregion": "Aleutian Ridge Volcanic Arc", "observatory": "avo", "usgs_region": "Alaska - Aleutians"},
        {"vnum": 1002, "name": "Bravo", "latitude": 15.0, "longitude": 145.0,
         "subregion": "Mariana Volcanic Arc", "observatory": None, "usgs_region": None},
        {"vnum": 1003, "name": "Charlie", "latitude": 45.0, "longitude": -121.0,
         "subregion": "High Cascades Volcanic Arc", "observatory": "cvo", "usgs_region": "Oregon"},
        {"vnum": 1004, "name": "Mount|Test", "latitude": 19.4, "longitude": -155.3,
         "subregion": "Hawaiian-Emperor Hotspot Volcano Group", "observatory": "hvo", "usgs_region": "Hawaii"},
        {"vnum": 1005, "name": "Echo", "latitude": 60.0, "longitude": -152.0,
         "subregion": "Aleutian Ridge Volcanic Arc", "observatory": "avo", "usgs_region": "Alaska - Cook Inlet"},
        {"vnum": 1006, "name": "Foxtrot", "latitude": 55.0, "longitude": -160.0,
         "subregion": "Alaska Peninsula Volcanic Arc", "observatory": "avo", "usgs_region": "Alaska Peninsula"},
    ])
    # GVP dates go back to year 1, before pandas' nanosecond range (1677): written with pyarrow in
    # microseconds, as Silver does
    import pyarrow as pa
    import pyarrow.parquet as pq

    when = lambda *a: datetime(*a, tzinfo=timezone.utc)  # noqa: E731
    ts = pa.timestamp("us", tz="UTC")
    (folder / "eruptions").mkdir(parents=True)
    pq.write_table(pa.table({
        "record_id": [f"eruptions:{i}" for i in range(5)],
        "processed_utc": pa.array([when(2026, 11, 23, 2)] * 5, ts),
        "vnum": [1002, 1003, 1004, 1005, 1006],
        # 1003: BCE; 1006: starts on a Monday at 00:00 UTC, as GVP day-precision dates do
        "start_time": pa.array([when(2024, 12, 23), None, when(2000, 6, 1), when(1510, 1, 1),
                                when(2026, 11, 2)], ts),
        "end_time": pa.array([when(2026, 3, 25), None, when(2001, 1, 1), None, when(2026, 11, 5)], ts),
        "is_continuing": [True, False, False, False, False],
        "vei": pa.array([None, 2, 1, 3, 1], pa.int64()),
    }), folder / "eruptions" / "eruptions_20261123_020000.parquet")
    quakes = ([("2026-09-08 10:00", 1001)] * 30 + [("2026-10-06 10:00", 1001)] * 10
              + [("2026-11-22 10:00", 1001)] * 6           # last complete week and the 7-day window
              + [("2026-11-23 01:00", 1001)] * 4           # current partial week: window only
              + [("2026-09-15 10:00", 1004)] * 5           # too few for a baseline
              + [("2026-09-22 10:00", 1006)] * 25)         # a baseline, so 1006 gets a weekly history
    _write(folder, "earthquakes", [{"nearest_vnum": v, "event_time": _ts(t)} for t, v in quakes])
    _write(folder, "alert_notices", [
        {"vnum": 1001, "alert_level": "ADVISORY", "color_code": "YELLOW", "sent_time": _ts("2026-11-01")},
        {"vnum": 1001, "alert_level": "WATCH", "color_code": "ORANGE", "sent_time": _ts("2026-11-20")},
    ])
    return read_silver(spark, folder)


@pytest.fixture
def gold(spark, silver):  # noqa: F811
    from volcano_pipeline.gold.indicator import build_indicator

    return build_indicator(spark, silver, WHEN, CFG).toPandas()


def _current(gold, vnum):
    rows = gold[(gold["section"] == "vai_current") & (gold["dimension"] == str(vnum))]
    return dict(zip(rows["metric"], rows["value"]))


def test_current_vai_for_alert_and_earthquakes(gold):
    a = _current(gold, 1001)
    # weekly counts 30, 10, 6 and nine zero weeks; 10 earthquakes in the window:
    # 10 weeks below (nine zeros and the 6), one equal -> p = 10.5 / 12 -> S = 0.75
    assert a["baseline_percentile"] == pytest.approx(10.5 / 12)
    assert a["seismic"] == pytest.approx(0.75) and a["reported"] == pytest.approx(0.7)
    assert math.isnan(a["eruptive"])                     # no dated eruption: not available
    score, used = combine({"reported": 0.7, "seismic": 0.75, "eruptive": None}, CFG["weights"])
    assert a["vai_score"] == pytest.approx(score) and a["components_used"] == len(used) == 2
    assert a["level_ord"] == level(score, CFG["levels"])[1]
    assert a["earthquakes_window"] == 10


def test_continuing_eruption_alone_gives_the_top_score(gold):
    b = _current(gold, 1002)
    assert b["eruptive"] == 1.0 and b["vai_score"] == 100.0 and b["components_used"] == 1


def test_volcano_with_nothing_usable_has_no_vai(gold):
    assert _current(gold, 1003) == {}


def test_old_eruption_decays(gold):
    d = _current(gold, 1004)
    years = (WHEN - datetime(2001, 1, 1, tzinfo=timezone.utc)).days / 365.25
    assert d["eruptive"] == pytest.approx(math.exp(-years / CFG["eruptive_decay_years"]), rel=1e-3)
    assert math.isnan(d["seismic"])                      # 5 earthquakes < 20: no baseline


def test_eruption_before_1677_still_counts(gold):
    """H of a volcano whose last eruption was in 1510 is tiny but available (it changes the weights)."""
    e = _current(gold, 1005)
    years = (WHEN - datetime(1510, 1, 1, tzinfo=timezone.utc)).days / 365.25
    assert e["eruptive"] == pytest.approx(math.exp(-years / CFG["eruptive_decay_years"]), rel=1e-3)
    assert e["components_used"] == 1


def test_eruption_starting_on_a_monday_does_not_reach_the_week_before(gold):
    weekly = gold[(gold["section"] == "vai_weekly") & (gold["dimension"] == "1006") & (gold["metric"] == "eruptive")]
    h = dict(zip(pd.to_datetime(weekly["period"]).dt.date, weekly["value"]))
    assert h[date(2026, 11, 2)] == 1.0
    assert math.isnan(h[date(2026, 10, 26)])          # no earlier eruption: not available, and not 1
    assert h[date(2026, 11, 9)] < 1.0                   # ended on 2026-11-05


def test_current_week_is_not_in_the_baseline(gold):
    weekly = gold[(gold["section"] == "vai_weekly") & (gold["dimension"] == "1001") & (gold["metric"] == "seismic")]
    assert len(weekly) == 12
    assert set(pd.to_datetime(weekly["period"]).dt.date) == {date(2026, 8, 31) + pd.Timedelta(weeks=i) for i in range(12)}


def test_weekly_history_only_for_volcanoes_with_a_baseline(gold):
    weekly = gold[gold["section"] == "vai_weekly"]
    assert set(weekly["dimension"]) == {"1001", "1006"}
    first = weekly[(weekly["dimension"] == "1001") & (weekly["metric"] == "seismic")
                   & (pd.to_datetime(weekly["period"]).dt.date == date(2026, 9, 7))]
    # the 30-earthquake week is above every other week: p = 11.5 / 12
    assert first["value"].item() == pytest.approx((11.5 / 12 - 0.5) / 0.5)


def test_map_rows_carry_name_region_and_alert(gold):
    m = gold[gold["section"] == "volcano_map"]
    dims = set(m["dimension"])
    assert "1001|Alpha|Alaska|avo" in dims
    assert "1002|Bravo|Northern Mariana Islands|" in dims          # region from the GVP subregion
    assert "1004|Mount/Test|Hawaii & American Samoa|hvo" in dims   # '|' in a name would break the split
    a = m[m["dimension"] == "1001|Alpha|Alaska|avo"].set_index("metric")["value"]
    assert a["alert_level_ord"] == 3 and a["color_code_ord"] == 3 and a["latitude"] == 52.0
    b = m[m["dimension"].str.startswith("1002|")].set_index("metric")["value"]
    assert math.isnan(b["alert_level_ord"])


def test_eruption_history_keeps_vei_and_continuing(gold):
    e = gold[(gold["section"] == "eruption_history") & (gold["dimension"] == "1002")].set_index("metric")
    assert e.loc["continuing", "value"] == 1.0 and math.isnan(e.loc["vei", "value"])
    assert pd.Timestamp(e.loc["vei", "period"]).date() == date(2024, 12, 23)
    # eruptions without a start date (BCE) are not in the history
    assert gold[(gold["section"] == "eruption_history") & (gold["dimension"] == "1003")].empty


def test_storytelling_includes_the_indicator_sections(spark, silver):  # noqa: F811
    pytest.importorskip("vaderSentiment")
    from volcano_pipeline.gold.storytelling import build_storytelling

    sections = {r["section"] for r in build_storytelling(spark, silver, WHEN).select("section").distinct().collect()}
    assert {"vai_current", "vai_weekly", "volcano_map", "eruption_history"} <= sections
