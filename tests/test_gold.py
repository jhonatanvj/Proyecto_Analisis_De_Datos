"""Gold layer (PySpark) on the recorded Workshop 2 Silver samples.

Spark 3.5 needs Java 8, 11 or 17. Where that is not available (e.g. Windows with a newer Java)
these tests are skipped; run them inside the Airflow image instead:

    docker compose run --rm airflow-scheduler python -m pytest -q tests/test_gold.py
"""
from __future__ import annotations

import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

SAMPLES = Path(__file__).resolve().parents[1] / "workshop_2" / "samples"
WHEN = datetime(2026, 11, 23, 3, 0, tzinfo=timezone.utc)


def _spark_or_skip():
    pytest.importorskip("pyspark")
    from volcano_pipeline.gold.spark import get_spark

    try:
        return get_spark("volcano-gold-tests")
    except Exception as exc:   # no compatible Java on this machine
        pytest.skip(f"Spark cannot start here ({type(exc).__name__}); run the tests in Docker")


@pytest.fixture(scope="module")
def spark():
    session = _spark_or_skip()
    yield session
    session.stop()


@pytest.fixture
def silver_dir(tmp_path):
    """The flat Workshop 2 samples copied into the Workshop 3 layout (one folder per topic)."""
    silver = tmp_path / "datalake_silver"
    for f in (SAMPLES / "silver").glob("*.parquet"):
        topic = f.name.rsplit("_", 2)[0]
        (silver / topic).mkdir(parents=True, exist_ok=True)
        shutil.copy(f, silver / topic / f.name)
    (silver / "quarantine").mkdir(parents=True)
    for f in (SAMPLES / "quarantine").glob("*.parquet"):
        shutil.copy(f, silver / "quarantine" / f.name)
    (silver / "_state").mkdir()
    (silver / "_state" / "seen_earthquakes.parquet").write_bytes(b"not read by Gold")
    return silver


def test_reader_gives_one_dataframe_per_topic(spark, silver_dir):
    from volcano_pipeline.gold.silver_reader import read_silver

    silver = read_silver(spark, silver_dir)
    assert silver["earthquakes"].count() == len(pd.read_parquet(silver_dir / "earthquakes"))
    assert "nearest_vnum" in silver["earthquakes"].columns
    assert silver["quarantine"] is not None


def test_conform_fills_optional_columns_and_rejects_missing_ones(spark):
    from volcano_pipeline.gold.contract import COLUMNS, conform

    df = spark.createDataFrame([("s", "m", 1)], "section string, metric string, value int")
    out = conform(df, WHEN)
    assert out.columns == COLUMNS
    row = out.first()
    assert row["value"] == 1.0 and row["dimension"] is None and row["generated_utc"] is not None
    with pytest.raises(ValueError):
        conform(df.drop("metric"), WHEN)


@pytest.mark.parametrize("kind", ["governance", "storytelling"])
def test_run_summary_writes_one_parquet_file_in_the_contract(spark, silver_dir, tmp_path, kind):
    from volcano_pipeline.gold.contract import COLUMNS
    from volcano_pipeline.gold.run import run_summary

    gold = tmp_path / "datalake_gold"
    summary = run_summary(kind, silver_dir, gold, WHEN, spark=spark)
    assert summary["file"] == f"{kind}_20261123_030000.parquet"
    assert [p.name for p in gold.iterdir()] == [summary["file"]]   # no temporary folders left
    df = pd.read_parquet(gold / summary["file"])
    assert list(df.columns) == COLUMNS and len(df) == summary["rows"] > 0


def test_volume_total_counts_every_topic(spark, silver_dir):
    from volcano_pipeline.gold.governance import volume_total
    from volcano_pipeline.gold.silver_reader import read_silver

    rows = {r["dimension"]: r["value"] for r in volume_total(spark, read_silver(spark, silver_dir)).collect()}
    assert rows["eruptions"] == len(pd.read_parquet(silver_dir / "eruptions"))
    assert set(rows) == {"volcanoes", "eruptions", "earthquakes", "alert_notices", "volcano_updates", "quarantine"}
    # notices: no file in the Workshop 2 samples, so the topic is absent, not an error


def test_reader_merges_old_and_new_silver_schemas(spark, tmp_path):
    """A topic folder can hold files written before and after a Silver column was added (Workshop 4
    added end_time / is_continuing); the reader must keep the new columns whichever file Spark samples."""
    from volcano_pipeline.gold.silver_reader import read_topic

    folder = tmp_path / "datalake_silver" / "eruptions"
    folder.mkdir(parents=True)
    pd.DataFrame({"record_id": ["a"], "vnum": [1]}).to_parquet(folder / "eruptions_20261101_000000.parquet")
    pd.DataFrame({"record_id": ["b"], "vnum": [2], "is_continuing": [True]}).to_parquet(
        folder / "eruptions_20261125_000000.parquet")
    df = read_topic(spark, tmp_path / "datalake_silver", "eruptions")
    assert "is_continuing" in df.columns and df.count() == 2
