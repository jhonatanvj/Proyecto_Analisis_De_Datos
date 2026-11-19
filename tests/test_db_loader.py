"""Silver -> PostgreSQL loader. Row preparation is tested on the recorded Silver samples, with no
database. The end-to-end test runs only when VOLCANO_TEST_DSN points to a disposable database
that already has sql/001-003 applied (it is skipped otherwise, so pytest never needs a server)."""
from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from volcano_pipeline.storage import loader

SAMPLES = Path(__file__).resolve().parents[1] / "workshop_2" / "samples"
SILVER = SAMPLES / "silver"


def _sample(topic: str) -> pd.DataFrame:
    return pd.read_parquet(next(SILVER.glob(f"{topic}_*.parquet")))


def test_silver_files_load_volcanoes_first_and_quarantine_last(tmp_path):
    for name in ["earthquakes/earthquakes_20261117_000000.parquet", "volcanoes/volcanoes_20261117_000000.parquet",
                 "eruptions_20261116_000000.parquet", "notes.txt", "seen_x.parquet", "_state/seen_x.parquet"]:
        (tmp_path / name).parent.mkdir(exist_ok=True)
        (tmp_path / name).touch()
    (tmp_path / "quarantine").mkdir()
    (tmp_path / "quarantine" / "rejected_20261117_000000.parquet").touch()
    order = [rel for _, rel, _ in loader.silver_files(tmp_path)]
    assert order == ["eruptions_20261116_000000.parquet", "volcanoes_20261117_000000.parquet",
                     "earthquakes_20261117_000000.parquet", "quarantine/rejected_20261117_000000.parquet"]


def test_source_comes_from_the_bronze_file_name():
    assert loader.source_of("usgs_fdsn_20261117_002309.json") == "usgs_fdsn"
    assert loader.source_of("usgs_feed_20261116_223830.json") == "usgs_feed"
    assert loader.source_of("weird.json", "usgs_feed") == "usgs_feed"
    assert loader.source_of(None) is None


def test_earthquakes_get_usgs_id_and_rows_without_coordinates_are_rejected():
    df = _sample("earthquakes")
    df.loc[0, "latitude"] = np.nan
    prepared = loader.prep_earthquakes(df)
    assert len(prepared.stage) == len(df) - 1
    assert (prepared.stage["record_id"] == "usgs:" + prepared.stage["usgs_id"]).all()
    assert set(prepared.stage["source_id"]) == {"usgs_feed"}
    [reject] = prepared.rejects
    assert reject["reason"] == "missing_coordinates"
    assert json.loads(reject["payload"])["record_id"] == df.loc[0, "record_id"]


def test_eruption_month_and_day_follow_time_precision():
    df = _sample("eruptions")
    stage = loader.prep_eruptions(df).stage
    by_precision = stage.groupby(stage["time_precision"].astype(str))
    day = by_precision.get_group("day")
    assert day["start_day"].notna().all() and day["start_month"].notna().all()
    year = by_precision.get_group("year")
    assert year["start_month"].isna().all() and year["start_day"].isna().all()


def test_unknown_colour_code_becomes_null():
    df = _sample("alert_notices")
    df["color_code"] = df["color_code"].astype(str)
    df.loc[0, "color_code"] = "PURPLE"
    stage = loader.prep_alert_notices(df).stage
    assert pd.isna(stage.loc[0, "color_code"])
    assert set(stage["color_code"].dropna()) <= loader.COLOR_CODES


def test_quarantine_payload_is_always_valid_json():
    df = pd.DataFrame({"source_id": [None, "usgs_feed"], "reason": ["unreadable_bronze", "x"],
                       "payload": ["OSError: disk", '{"id": 1}'],
                       "bronze_file": ["usgs_hans_20261116_000000.json", "usgs_feed_20261116_000000.json"],
                       "processed_utc": pd.to_datetime(["2026-11-16", "2026-11-16"], utc=True)})
    stage = loader.prep_quarantine(df).stage
    assert [json.loads(p) for p in stage["payload"]] == ["OSError: disk", {"id": 1}]
    assert list(stage["source_id"]) == ["usgs_hans", "usgs_feed"]


def test_watermarks_are_fixed_width_utc_strings_per_source():
    df = loader.prep_earthquakes(_sample("earthquakes")).stage
    marks = loader.watermark_candidates("earthquakes", df, {})
    kind, value = marks["usgs_feed"]
    assert kind == "time" and value == loader.format_ts(df["updated_time"].max())
    assert len(value) == len("2026-11-16T22:27:10.828000Z")
    cfg = {"sources": [{"source_id": "gvp_volcanoes", "database_version": "5.4.0"}]}
    vol = loader.prep_volcanoes(_sample("volcanoes")).stage
    assert loader.watermark_candidates("volcanoes", vol, cfg) == {"gvp_volcanoes": ("version", "5.4.0")}
    assert loader.watermark_candidates("quarantine", vol, cfg) == {}


def test_py_values_turn_numpy_and_missing_values_into_python():
    assert loader.py_values(pd.Series([1, None], dtype="Int64")) == [1, None]
    assert loader.py_values(pd.Series([np.array(["ash", "lava"]), None])) == [["ash", "lava"], None]
    old = np.array(["1500-01-01", "NaT"], dtype="datetime64[us]")   # before 1677: needs microseconds
    ts = loader.py_values(pd.Series(old).dt.tz_localize("UTC"))
    assert ts[0].year == 1500 and ts[1] is None
    assert loader.py_values(pd.Series(["a", np.nan], dtype="category")) == ["a", None]


@pytest.mark.skipif(not os.getenv("VOLCANO_TEST_DSN"), reason="needs a disposable PostGIS database")
def test_loading_twice_creates_no_duplicates(tmp_path):
    import psycopg

    from volcano_pipeline.config import load_config

    shutil.copytree(SILVER, tmp_path / "silver")
    with psycopg.connect(os.environ["VOLCANO_TEST_DSN"]) as conn:
        first = loader.load_pending(tmp_path / "silver", load_config(), "historical", conn=conn)
        assert first["failed"] is None
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM event")
            events = cur.fetchone()[0]
        # same rows again under a newer file name: nothing new, nothing updated
        shutil.copy(next(SILVER.glob("earthquakes_*.parquet")), tmp_path / "silver" / "earthquakes_29991231_000000.parquet")
        second = loader.load_pending(tmp_path / "silver", load_config(), "incremental", conn=conn)
        assert [(r["new"], r["updated"]) for r in second["loaded"]] == [(0, 0)]
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM event")
            assert cur.fetchone()[0] == events
