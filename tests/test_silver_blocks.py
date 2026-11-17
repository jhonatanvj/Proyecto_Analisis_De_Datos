"""Silver building blocks: schemas, outlier flags, text cleaning, dedup state and Parquet IO."""
from datetime import datetime, timezone

import pandas as pd

from volcano_pipeline.silver.io import write_quarantine, write_topic
from volcano_pipeline.silver.quality import iqr_outliers, robust_z_outliers
from volcano_pipeline.silver.schemas import LINEAGE, SCHEMAS, enforce
from volcano_pipeline.silver.state import filter_unseen, load_manifest, mark_processed, update_seen
from volcano_pipeline.silver.text import clean_text, keyword_flags, load_stopwords, tokenize

WHEN = datetime(2026, 11, 16, 3, 15, 0, tzinfo=timezone.utc)


def test_clean_text_keeps_unicode_and_drops_html():
    assert clean_text("<p>Kīlauea&nbsp;summit, see https://x.gov/a.</p>") == "kīlauea summit see"


def test_clean_text_empty():
    assert clean_text("") == "" and clean_text(None) == ""


def test_tokenize_drops_stopwords_short_and_numeric_tokens():
    assert tokenize("the lava is erupting at the summit 56 a", {"the", "is", "at"}) == ["lava", "erupting", "summit"]


def test_stopwords_file_loads():
    sw = load_stopwords()
    assert {"the", "and", "of"} <= sw and "lava" not in sw
    assert "#" not in sw and "english" not in sw   # the header comment is not a stop word


def test_keyword_flags_use_word_stems():
    flags = keyword_flags(["lava", "erupting", "summit"])
    assert flags["kw_eruption"] and flags["kw_lava"] and not flags["kw_ash"]
    assert set(flags) == {"kw_eruption", "kw_ash", "kw_lava", "kw_seismicity", "kw_deformation", "kw_gas", "kw_plume"}


def test_iqr_and_z_flags():
    s = pd.Series([1.0, 1.1, 1.2, 1.0, 0.9, 6.5, None])
    assert iqr_outliers(s).tolist() == [False] * 5 + [True, False]
    z = robust_z_outliers(s)
    assert z.iloc[5] and not z.iloc[:5].any() and not z.iloc[6]


def test_z_flags_constant_series():
    assert not robust_z_outliers(pd.Series([2.0, 2.0, 2.0, 2.0])).any()


def test_seen_filter_roundtrip(tmp_path):
    df = pd.DataFrame({"record_id": ["a", "a", "b"], "content_hash": ["1", "1", "2"]})
    first = filter_unseen(df, tmp_path, "t")
    assert len(first) == 2
    update_seen(first, tmp_path, "t")
    assert filter_unseen(df, tmp_path, "t").empty
    changed = pd.DataFrame({"record_id": ["a"], "content_hash": ["9"]})
    assert len(filter_unseen(changed, tmp_path, "t")) == 1


def test_seen_filter_keeps_last_revision_in_batch(tmp_path):
    df = pd.DataFrame({"record_id": ["a", "a"], "content_hash": ["1", "2"]})
    out = filter_unseen(df, tmp_path, "t")
    assert out["content_hash"].tolist() == ["2"]


def test_manifest_roundtrip(tmp_path):
    assert load_manifest(tmp_path) == set()
    mark_processed(tmp_path, ["b.json", "a.json"])
    mark_processed(tmp_path, ["a.json"])
    assert load_manifest(tmp_path) == {"a.json", "b.json"}


def test_enforce_adds_lineage_and_casts():
    df = pd.DataFrame({"record_id": ["usgs:x"], "content_hash": ["h"], "mag": ["1.5"],
                       "mag_type": ["ml"], "event_time": ["2026-11-16T01:00:00Z"], "extra": [1]})
    out = enforce(df, "earthquakes")
    assert list(out.columns) == [f.name for f in SCHEMAS["earthquakes"]]
    assert set(LINEAGE) <= set(out.columns) and "extra" not in out.columns
    assert out["mag"].dtype == "float64" and str(out["mag_type"].dtype) == "category"
    assert str(out["event_time"].dtype) == "datetime64[ns, UTC]"


def test_write_topic_and_quarantine(tmp_path):
    df = enforce(pd.DataFrame({"record_id": ["usgs:x"], "content_hash": ["h"], "mag": [1.0]}), "earthquakes")
    p = write_topic(df, tmp_path, "earthquakes", WHEN)
    assert p.name == "earthquakes_20261116_031500.parquet"
    assert pd.read_parquet(p)["record_id"].tolist() == ["usgs:x"]
    assert write_topic(df.iloc[0:0], tmp_path, "earthquakes", WHEN) is None
    q = write_quarantine([{"source_id": "usgs_feed", "reason": "missing_coordinates",
                           "payload": {"id": "x"}, "bronze_file": "f.json"}], tmp_path, WHEN)
    assert q.parent.name == "quarantine" and q.name == "rejected_20261116_031500.parquet"
    assert pd.read_parquet(q)["payload"].iloc[0] == '{"id": "x"}'
    assert write_quarantine([], tmp_path, WHEN) is None


def test_parquet_timestamps_are_microseconds(tmp_path):
    # Spark 3 cannot read Parquet TIMESTAMP(NANOS); Workshop 3 reads this folder with PySpark
    import pyarrow.parquet as pq
    df = enforce(pd.DataFrame({"record_id": ["usgs:x"], "content_hash": ["h"],
                               "event_time": ["2026-11-16T01:00:00Z"], "processed_utc": [WHEN]}), "earthquakes")
    p = write_topic(df, tmp_path, "earthquakes", WHEN)
    schema = pq.ParquetFile(p).schema.to_arrow_schema()
    units = {f.name: f.type.unit for f in schema if str(f.type).startswith("timestamp")}
    assert units and set(units.values()) == {"us"}
    q = write_quarantine([{"source_id": "s", "reason": "r", "payload": {}}], tmp_path, WHEN)
    assert pq.ParquetFile(q).schema.to_arrow_schema().field("processed_utc").type.unit == "us"
