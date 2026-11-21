"""Silver runner: idempotency, cross-batch dedup, quarantine and lineage."""
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from volcano_pipeline.silver.run import process_bronze_files, unprocessed_files

FIX = Path(__file__).parent / "fixtures" / "bronze"
T1 = datetime(2026, 11, 16, 3, 15, 0, tzinfo=timezone.utc)
T2 = datetime(2026, 11, 16, 4, 15, 0, tzinfo=timezone.utc)
T3 = datetime(2026, 11, 16, 5, 15, 0, tzinfo=timezone.utc)


def bronze_copy(tmp_path):
    bronze = tmp_path / "bronze"
    bronze.mkdir()
    for f in FIX.glob("*.json"):
        shutil.copy(f, bronze / f.name)
    return bronze


def feed_copy(bronze, name, edit=None):
    env = json.loads((bronze / "usgs_feed_20261116_010000.json").read_text(encoding="utf-8"))
    if edit:
        edit(env)
    (bronze / name).write_text(json.dumps(env), encoding="utf-8")


def test_unprocessed_files_ignores_partial_and_tmp(tmp_path):
    bronze = bronze_copy(tmp_path)
    (bronze / "usgs_feed_20261116_020000.json.part").write_text("{")
    (bronze / "_tmp").mkdir()
    names = [p.name for p in unprocessed_files(bronze, tmp_path / "silver")]
    assert len(names) == 5 and names == sorted(names, key=lambda n: n[-20:])


def test_idempotent_second_run(tmp_path):
    bronze, silver = bronze_copy(tmp_path), tmp_path / "silver"
    first = process_bronze_files(bronze, silver, when=T1)
    assert set(first["written"]) == {"earthquakes", "alert_notices", "volcanoes", "eruptions", "volcano_updates"}
    assert first["rejected"] > 0 and first["out_of_scope"] == 1
    again = process_bronze_files(bronze, silver, when=T2)
    assert again["processed"] == [] and again["written"] == {}
    assert len(list((silver / "earthquakes").glob("earthquakes_*.parquet"))) == 1


def test_overlap_between_feed_files(tmp_path):
    bronze, silver = bronze_copy(tmp_path), tmp_path / "silver"
    process_bronze_files(bronze, silver, when=T1)
    feed_copy(bronze, "usgs_feed_20261116_020000.json")             # same week window, nothing new
    assert "earthquakes" not in process_bronze_files(bronze, silver, when=T2)["written"]

    def revise(env):
        env["payload"]["features"][0]["properties"]["updated"] += 60_000
        env["payload"]["features"][0]["properties"]["status"] = "reviewed"
    feed_copy(bronze, "usgs_feed_20261116_030000.json", revise)
    out = process_bronze_files(bronze, silver, when=T3)
    df = pd.read_parquet(silver / out["written"]["earthquakes"])
    assert len(df) == 1 and df["review_status"].astype(str).item() == "reviewed"


def test_overlapping_feed_does_not_requarantine(tmp_path):
    bronze, silver = bronze_copy(tmp_path), tmp_path / "silver"
    process_bronze_files(bronze, silver, when=T1)
    feed_copy(bronze, "usgs_feed_20261116_020000.json")             # same quarry blast, same missing geometry
    out = process_bronze_files(bronze, silver, when=T2)
    assert out["rejected"] == 0
    assert len(list((silver / "quarantine").glob("rejected_*.parquet"))) == 1


def test_corrupt_bronze_quarantined(tmp_path):
    bronze, silver = bronze_copy(tmp_path), tmp_path / "silver"
    (bronze / "usgs_hans_20261116_020000.json").write_text('{"broken', encoding="utf-8")
    out = process_bronze_files(bronze, silver, when=T1)
    assert "usgs_hans_20261116_020000.json" in out["processed"]
    q = pd.read_parquet(next((silver / "quarantine").glob("rejected_*.parquet")))
    assert (q.loc[q["bronze_file"] == "usgs_hans_20261116_020000.json", "reason"] == "unreadable_bronze").all()
    assert "earthquakes" in out["written"]


def test_unknown_source_quarantined(tmp_path):
    bronze, silver = tmp_path / "bronze", tmp_path / "silver"
    bronze.mkdir()
    (bronze / "mystery_20261116_010000.json").write_text('{"source_id": "mystery", "payload": []}')
    out = process_bronze_files(bronze, silver, when=T1)
    assert out["processed"] == ["mystery_20261116_010000.json"] and out["rejected"] == 1


def test_lineage_columns(tmp_path):
    bronze, silver = bronze_copy(tmp_path), tmp_path / "silver"
    out = process_bronze_files(bronze, silver, when=T1)
    df = pd.read_parquet(silver / out["written"]["earthquakes"])
    assert set(df["bronze_file"]) == {"usgs_feed_20261116_010000.json"}
    assert df["bronze_sha256"].str.len().eq(64).all()
    assert (df["processed_utc"] == pd.Timestamp(T1)).all()
    assert out["written"]["earthquakes"] == "earthquakes/earthquakes_20261116_031500.parquet"


def test_crash_after_writing_does_not_duplicate_rows(tmp_path, monkeypatch):
    import volcano_pipeline.silver.run as run
    bronze, silver = bronze_copy(tmp_path), tmp_path / "silver"

    def boom(*args, **kwargs):
        raise RuntimeError("disk full")
    monkeypatch.setattr(run, "update_seen", boom)
    try:
        process_bronze_files(bronze, silver, when=T1)
    except RuntimeError:
        pass
    monkeypatch.undo()
    process_bronze_files(bronze, silver, when=T2)
    files = list((silver / "earthquakes").glob("earthquakes_*.parquet"))
    assert len(files) == 1 and files[0].name == "earthquakes_20261116_041500.parquet"


def test_transform_error_quarantines_file_and_continues(tmp_path):
    bronze, silver = bronze_copy(tmp_path), tmp_path / "silver"
    (bronze / "usgs_feed_20261116_020000.json").write_text(
        '{"source_id": "usgs_feed", "payload": [1, 2]}', encoding="utf-8")
    out = process_bronze_files(bronze, silver, when=T1)
    assert "usgs_feed_20261116_020000.json" in out["processed"]
    q = pd.read_parquet(next((silver / "quarantine").glob("rejected_*.parquet")))
    bad = q[q["bronze_file"] == "usgs_feed_20261116_020000.json"]
    assert bad["reason"].tolist() == ["transform_error"] and "AttributeError" in bad["payload"].item()
    assert "earthquakes" in out["written"]
