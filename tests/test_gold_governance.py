"""Governance indicators (Workshop 3, Part 2) on small hand-made DataFrames with known answers.

Spark needs Java 8/11/17; without it these tests are skipped (see tests/test_gold.py). Inside Docker:
    docker compose exec airflow-scheduler python -m pytest -q tests/test_gold_governance.py
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

import pandas as pd
import pyarrow.parquet as pq
import pytest

from test_gold import WHEN, silver_dir, spark  # noqa: F401  (reuse the Spark session and Silver samples)

T0 = datetime(2026, 11, 2, 10, 0, tzinfo=timezone.utc)      # a Monday
T1 = datetime(2026, 11, 3, 10, 0, tzinfo=timezone.utc)      # same week
T2 = datetime(2026, 11, 10, 10, 0, tzinfo=timezone.utc)     # next week
P1, P2 = datetime(2026, 11, 17, 1, 0, tzinfo=timezone.utc), datetime(2026, 11, 18, 1, 0, tzinfo=timezone.utc)


def values(df, metric=None) -> dict:
    """{dimension: value} of a KPI result, optionally for one metric."""
    rows = df.collect() if metric is None else df.where(df.metric == metric).collect()
    return {r["dimension"]: r["value"] for r in rows}


def test_volume_weekly_counts_the_newest_version_per_week(spark):
    from volcano_pipeline.gold.governance import volume_weekly

    feed, fdsn = "usgs_feed_20261119_200000.json", "usgs_fdsn_20261119_200000.json"
    eq = spark.createDataFrame(
        [("a", T0, P1, feed), ("a", T0, P2, fdsn), ("b", T1, P1, feed), ("c", T2, P1, feed)],  # "a": 2 versions
        "record_id string, event_time timestamp, processed_utc timestamp, bronze_file string")
    out = volume_weekly(spark, {"earthquakes": eq, "notices": None})
    # cast in Spark (session time zone UTC): collect() would shift timestamps to the local time zone
    rows = (out.where("metric = 'records'")
            .selectExpr("CAST(period AS DATE) AS day", "value", "dimension").orderBy("day").collect())
    assert [(str(r["day"]), r["value"], r["dimension"]) for r in rows] == [
        ("2026-11-02", 2.0, "earthquakes"), ("2026-11-09", 1.0, "earthquakes")]
    by_source = {(str(r["day"]), r["dimension"]): r["value"] for r in out.where("metric = 'records_by_source'")
                 .selectExpr("CAST(period AS DATE) AS day", "dimension", "value").collect()}
    # the newest version of "a" came from FDSN
    assert by_source == {("2026-11-02", "usgs_fdsn"): 1.0, ("2026-11-02", "usgs_feed"): 1.0,
                         ("2026-11-09", "usgs_feed"): 1.0}
    assert volume_weekly(spark, {"earthquakes": None, "notices": None}) is None


def test_null_rate_counts_nulls_blank_strings_and_empty_lists(spark):
    from volcano_pipeline.gold.governance import null_rate

    notices = spark.createDataFrame(
        [("a", P1, None, ["x"]), ("b", P1, "  ", []), ("c", P1, "ok", ["y"]), ("d", P1, "ok", ["z"])],
        "record_id string, processed_utc timestamp, region string, tokens array<string>")
    rates = values(null_rate(spark, {"notices": notices}))
    assert rates["notices.region"] == 50.0          # one null + one blank of four
    assert rates["notices.tokens"] == 25.0          # one empty list
    assert rates["notices.record_id"] == 0.0


def test_duplicate_rate_splits_exact_copies_same_value_copies_and_revisions(spark):
    from volcano_pipeline.gold.governance import duplicate_rate

    eq = spark.createDataFrame([
        ("a", "h1", 1.5, P1, True), ("a", "h1", 1.5, P2, False),   # exact copy (outlier flag ignored)
        ("b", "h1", 2.0, P1, False), ("b", "h2", 2.0, P2, False),  # same values, another hash
        ("c", "h1", 3.0, P1, False), ("c", "h2", 3.1, P2, False),  # real revision
        ("d", "h1", 1.0, P1, False), ("e", "h1", 1.0, P1, False),
    ], "record_id string, content_hash string, mag double, processed_utc timestamp, is_outlier_mag_iqr boolean")
    out = duplicate_rate(spark, {"earthquakes": eq})
    assert values(out, "pct_exact_copies") == {"earthquakes": 12.5}          # 1 of 8 rows
    assert values(out, "pct_same_value_copies") == {"earthquakes": 25.0}     # a and b
    assert values(out, "pct_real_revisions") == {"earthquakes": 12.5}        # c
    assert out.where("metric LIKE '%before_dedup'").count() == 0             # no Bronze next to these rows


def test_duplicate_rate_counts_repeated_fdsn_ids_in_processed_bronze_files(spark, tmp_path):
    from volcano_pipeline.gold.governance import duplicate_rate

    bronze, silver = tmp_path / "datalake_bronze", tmp_path / "datalake_silver"
    bronze.mkdir()
    (silver / "_state").mkdir(parents=True)
    batches = {"usgs_fdsn_20261119_220000.json": ["e1", "e2", "e3"],
               "usgs_fdsn_20261119_220100.json": ["e2", "e3", "e4"],      # overlapping query
               "usgs_fdsn_20261119_230000.json": ["e9"]}                  # not processed yet
    for name, ids in batches.items():
        envelope = {"source_id": "usgs_fdsn", "record_count": len(ids),
                    "payload": {"type": "FeatureCollection", "features": [{"id": i, "type": "Feature"} for i in ids]}}
        (bronze / name).write_text(json.dumps(envelope))
    (silver / "_state" / "manifest.json").write_text(json.dumps(sorted(batches)[:2]))
    eq_dir = silver / "earthquakes"
    spark.createDataFrame([("e1", "h", P1)], "record_id string, content_hash string, processed_utc timestamp") \
        .write.parquet(str(eq_dir))
    out = duplicate_rate(spark, {"earthquakes": spark.read.parquet(str(eq_dir))})
    assert values(out, "records_before_dedup") == {"usgs_fdsn": 6.0}
    assert values(out, "repeated_before_dedup") == {"usgs_fdsn": 2.0}
    assert values(out, "pct_repeated_before_dedup")["usgs_fdsn"] == pytest.approx(100 * 2 / 6)


def test_schema_compliance_compares_each_file_with_the_contract(spark, tmp_path):
    from volcano_pipeline.gold.governance import schema_compliance
    from volcano_pipeline.silver.schemas import SCHEMAS, enforce, to_table

    folder = tmp_path / "datalake_silver" / "volcanoes"
    folder.mkdir(parents=True)
    table = to_table(enforce(pd.DataFrame({"record_id": ["v1"], "vnum": [1]}), "volcanoes"), SCHEMAS["volcanoes"])
    # written like silver/io.py does: microsecond timestamps, so Spark can read them
    pq.write_table(table, folder / "volcanoes_20261117_000000.parquet", coerce_timestamps="us",
                   allow_truncated_timestamps=True)
    pq.write_table(table.drop(["country"]), folder / "volcanoes_20261118_000000.parquet",
                   coerce_timestamps="us", allow_truncated_timestamps=True)
    out = schema_compliance(spark, {"volcanoes": spark.read.parquet(str(folder))})
    assert values(out, "pct_files_conforming") == {"volcanoes": 50.0}
    assert values(out, "files") == {"volcanoes": 2.0}


def test_quarantine_rate_per_source_with_categorised_reasons(spark):
    from volcano_pipeline.gold.governance import quarantine_rate

    eq = spark.createDataFrame([(f"e{i}", "usgs_fdsn_20261119_220000.json") for i in range(9)],
                               "record_id string, bronze_file string")
    q = spark.createDataFrame([
        ("usgs_fdsn", "non_tectonic_type", "{}", "usgs_fdsn_20261119_220000.json", P1),
        ("usgs_hans_notices", "missing_id_or_time", "{}", "usgs_hans_notices_20261119_220000.json", P1),
        ("usgs_hans_volcanoes", "unknown_source", "{}", "usgs_hans_volcanoes_20261119_220000.json", P1),
    ], "source_id string, reason string, payload string, bronze_file string, processed_utc timestamp")
    out = quarantine_rate(spark, {"earthquakes": eq, "quarantine": q})
    rates = values(out, "pct_quarantined")
    assert rates["usgs_fdsn"] == 10.0                                   # 1 of 10 processed
    assert rates["usgs_hans_notices"] == 100.0
    assert rates["all sources"] == pytest.approx(100 * 3 / 12)
    assert values(out, "records_by_reason") == {
        "usgs_fdsn|non_tectonic_type|business_rule": 1.0,
        "usgs_hans_notices|missing_id_or_time|source_defect": 1.0,
        "usgs_hans_volcanoes|unknown_source|pipeline_error": 1.0}


def test_outlier_rate_uses_per_volcano_fences_with_a_global_fallback(spark):
    from volcano_pipeline.gold.governance import outlier_rate

    # volcano 1 (41 events): 20 x M1.0, 20 x M1.2 and one M2.0 -> its own fences [0.7, 1.5] flag the M2.0
    # volcano 2 (3 events, under 30): M5.0 x 3 -> global fences [0.7, 1.5] flag all three
    mags = [(1, 1.0)] * 20 + [(1, 1.2)] * 20 + [(1, 2.0)] + [(2, 5.0)] * 3
    eq = spark.createDataFrame(
        [(f"e{i}", P1, vnum, mag, 5.0, False, False) for i, (vnum, mag) in enumerate(mags)],
        "record_id string, processed_utc timestamp, nearest_vnum long, mag double, depth_km double, "
        "is_outlier_mag_iqr boolean, is_outlier_depth_iqr boolean")
    out = outlier_rate(spark, {"earthquakes": eq})
    assert values(out, "pct_outlier")["earthquakes.mag"] == pytest.approx(100 * 4 / 44)
    assert values(out, "pct_outlier")["earthquakes.depth_km"] == 0.0     # constant depth
    assert values(out, "pct_outlier_silver_flags")["earthquakes.mag"] == 0.0


def test_text_length_per_observatory_and_encoding_errors(spark):
    from volcano_pipeline.gold.governance import text_length

    notices = spark.createDataFrame([
        ("a", P1, T0, "hvo", 100, "kilauea continued to erupt"),
        ("b", P1, T1, "hvo", 300, "kä lauea continued to erupt"),          # mojibake
        ("c", P1, T2, "avo", 200, "the volcano’s summit"),                 # real apostrophe: fine
        ("d", P1, T2, "avo", 400, "társilo girona, décollement"),          # legitimate accents
    ], "record_id string, processed_utc timestamp, sent_time timestamp, observatory string, "
       "text_length long, text_clean string")
    out = text_length(spark, {"notices": notices})
    all_time = out.where("period IS NULL")
    assert values(all_time, "max_chars")["hvo"] == 300.0
    assert values(all_time, "mean_chars")["notices"] == 250.0
    assert values(out, "pct_encoding_errors") == {"notices": 25.0}
    assert out.where("period IS NOT NULL").select("period").distinct().count() == 1   # all in 2026


def test_freshness_and_ingestion_frequency_from_the_lineage(spark):
    from volcano_pipeline.gold.governance import freshness, ingestion_frequency

    files = ["usgs_feed_20261119_200000.json", "usgs_feed_20261119_200500.json",   # same hour
             "usgs_feed_20261119_210000.json", "usgs_hans_20261118_100000.json"]
    eq = spark.createDataFrame([(f"e{i}", f) for i, f in enumerate(files)], "record_id string, bronze_file string")
    silver = {"earthquakes": eq, "quarantine": None}
    hours = values(ingestion_frequency(spark, silver), "hours_with_file")
    assert hours == {"usgs_feed": 2.0, "usgs_hans": 1.0}
    expected = values(ingestion_frequency(spark, silver), "expected_per_day")
    assert expected["usgs_feed"] == 24.0                     # schedule from config/sources.yaml
    age = values(freshness(spark, silver))
    assert age["usgs_hans"] > age["usgs_feed"] > 0


def test_generated_utc_is_the_run_time_in_utc_whatever_the_host_time_zone(spark):
    from volcano_pipeline.gold.contract import conform

    df = conform(spark.createDataFrame([("s", "m", 1.0)], "section string, metric string, value double"), WHEN)
    # formatted inside Spark (session time zone UTC): 03:00 UTC, not shifted by the host offset
    assert df.selectExpr("date_format(generated_utc, 'yyyy-MM-dd HH:mm') AS t").first()["t"] == "2026-11-23 03:00"


def test_build_governance_on_the_silver_samples_has_every_section(spark, silver_dir):
    from volcano_pipeline.gold.governance import build_governance
    from volcano_pipeline.gold.silver_reader import read_silver

    df = build_governance(spark, read_silver(spark, silver_dir), WHEN)
    sections = {r["section"] for r in df.select("section").distinct().collect()}
    assert sections == {"volume_total", "volume_weekly", "null_rate", "duplicate_rate", "schema_compliance",
                        "quarantine_rate", "outlier_rate", "text_length", "freshness", "ingestion_frequency",
                        "data_health", "kpi_summary"}
    assert df.where("value IS NULL AND section NOT IN ('ingestion_frequency', 'kpi_summary')").count() == 0
    kpis = {r["dimension"] for r in df.where("section = 'kpi_summary' AND metric = 'status'").collect()}
    assert kpis == {"null_rate", "duplicate_rate", "schema_compliance", "quarantine_rate", "outlier_rate",
                    "freshness", "ingestion_frequency"} | ({"text_length"} if "text_length" in kpis else set())


# ------------------------------------------------------------------------------ Workshop 4

def test_data_health_per_source(spark):
    from volcano_pipeline.gold.governance import data_health

    eq = spark.createDataFrame([
        ("e1", "usgs_feed_20261119_200000.json", T0), ("e2", "usgs_feed_20261119_210000.json", T2),
        ("e3", "usgs_fdsn_20261118_100000.json", T1)],
        "record_id string, bronze_file string, updated_time timestamp")
    q = spark.createDataFrame([("usgs_feed", "non_tectonic_type", "{}", "usgs_feed_20261119_210000.json", P1)],
                              "source_id string, reason string, payload string, bronze_file string, "
                              "processed_utc timestamp")
    out = data_health(spark, {"earthquakes": eq, "quarantine": q})
    assert values(out, "records_in_silver") == {"usgs_feed": 2.0, "usgs_fdsn": 1.0}
    assert values(out, "rejected_records") == {"usgs_feed": 1.0}
    last = values(out, "last_file_utc")
    assert last["usgs_feed"] == datetime(2026, 11, 19, 21, 0, tzinfo=timezone.utc).timestamp()
    age = values(out, "watermark_age_hours")
    assert age["usgs_fdsn"] > age["usgs_feed"] > 0              # fdsn's newest update (T1) is older than T2


def _gold_rows(spark, rows):
    """Conformed Gold rows (section, metric, dimension, period, value) as kpi_summary receives them."""
    from volcano_pipeline.gold.contract import conform

    df = spark.createDataFrame(rows, "section string, metric string, dimension string, period date, value double")
    return conform(df, WHEN)


def test_kpi_summary_headlines_thresholds_and_status(spark):
    from datetime import date

    from volcano_pipeline.gold.governance import kpi_summary

    rows = _gold_rows(spark, [
        ("volume_total", "records", "earthquakes", None, 900.0),
        ("volume_total", "records", "notices", None, 100.0),
        ("volume_total", "records", "quarantine", None, 50.0),           # not counted in the volume
        ("null_rate", "pct_null", "notices.region", None, 10.0),
        ("null_rate", "pct_null", "notices.vnum", None, 0.0),
        ("duplicate_rate", "pct_exact_copies", "earthquakes", None, 1.0),
        ("duplicate_rate", "pct_exact_copies", "notices", None, 0.0),
        ("schema_compliance", "pct_files_conforming", "earthquakes", None, 100.0),
        ("schema_compliance", "pct_files_conforming", "notices", None, 50.0),
        ("quarantine_rate", "pct_quarantined", "all sources", None, 0.5),
        ("outlier_rate", "pct_outlier", "earthquakes.mag", None, 3.0),
        ("outlier_rate", "pct_outlier", "earthquakes.depth_km", None, 9.0),
        ("text_length", "pct_encoding_errors", "notices", None, 0.05),
        ("freshness", "hours_since_last", "usgs_feed", None, 1.0),          # hourly: fresh below 2 h
        ("freshness", "hours_since_last", "usgs_hans", None, 30.0),         # hourly: stale
        # WHEN is 2026-11-23: the last 7 full days are 11-16 ... 11-22
        ("ingestion_frequency", "hours_with_file", "usgs_feed", date(2026, 11, 16), 24.0),
        ("ingestion_frequency", "hours_with_file", "usgs_feed", date(2026, 11, 22), 24.0),
        ("ingestion_frequency", "hours_with_file", "usgs_feed", date(2026, 11, 23), 24.0),   # today: ignored
    ])
    thresholds = {"null_rate": {"rule": "max", "value": 5.0}, "duplicate_rate": {"rule": "max", "value": 0.0},
                  "schema_compliance": {"rule": "min", "value": 100.0},
                  "quarantine_rate": {"rule": "max", "value": 2.0}, "outlier_rate": {"rule": "max", "value": 15.0},
                  "text_length": {"rule": "max", "value": 0.1}, "freshness": {"rule": "min", "value": 100.0},
                  "ingestion_frequency": {"rule": "min", "value": 80.0}}
    out = kpi_summary(spark, rows, thresholds)
    value = {r["dimension"]: r["value"] for r in out.where("metric NOT LIKE 'threshold_%' AND metric != 'status'")
             .collect()}
    assert value["volume"] == 1000.0
    assert value["null_rate"] == 5.0                                    # mean of 10 and 0
    assert value["duplicate_rate"] == pytest.approx(0.9)                # (1 % x 900) / 1000 rows
    assert value["schema_compliance"] == 50.0 and value["outlier_rate"] == 9.0
    assert value["freshness"] == 50.0                                   # 1 of 2 sources fresh
    assert value["ingestion_frequency"] == pytest.approx(100 * 48 / 168)
    status = values(out, "status")
    assert status == {"null_rate": 1.0, "duplicate_rate": 0.0, "schema_compliance": 0.0, "quarantine_rate": 1.0,
                      "outlier_rate": 1.0, "text_length": 1.0, "freshness": 0.0, "ingestion_frequency": 0.0}
    assert values(out, "threshold_min")["schema_compliance"] == 100.0
    assert "volume" not in status                                       # no threshold: value only


def test_every_kpi_has_a_threshold_in_sources_yaml():
    from volcano_pipeline.config import load_config

    thresholds = load_config()["governance"]["thresholds"]
    assert set(thresholds) == {"null_rate", "duplicate_rate", "schema_compliance", "quarantine_rate",
                               "outlier_rate", "text_length", "freshness", "ingestion_frequency"}
    assert all(t["rule"] in ("max", "min") and isinstance(t["value"], (int, float)) for t in thresholds.values())
