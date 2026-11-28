"""Governance summary: data-quality indicators of the Silver layer (Workshop 3, Part 2).

build_governance() receives the Silver topics read by silver_reader and returns rows in the Gold
contract (see contract.py). Each indicator is one function returning a DataFrame with section,
metric, value (plus dimension / period / unit), or None when its input topics have no data, so it
can be added, tested and explained on its own. Every indicator is justified by findings of
notebooks/03_data_quality_findings.ipynb (ids F01-F24 in its section 9).

Data-quality KPIs:
* null_rate: % of null, blank or empty values per field, newest version of each record.
* duplicate_rate: before dedup (FDSN ids in Bronze: overlapping radial queries) and after dedup
  (Silver exact copies, copies with the same values but another hash, and real revisions).
* schema_compliance: % of Silver files whose columns and types match silver/schemas.py.
* quarantine_rate: % of processed records sent to quarantine per source; the reasons, grouped as
  business_rule / source_defect / pipeline_error, are a breakdown of the same section.
* outlier_rate: % of earthquakes outside IQR fences computed per volcano over all its records
  (global fences for volcanoes with fewer than 30 earthquakes); Silver's per-file flags for comparison.
* text_length: characters of the HANS notices per observatory and year, plus the sub-metric
  pct_encoding_errors (mojibake).

Operational metrics:
* volume_total / volume_weekly: records per topic, all-time and per week.
* freshness: hours since the newest Bronze file that reached Silver, per source.
* ingestion_frequency: hours with a new Bronze file per day vs the schedule in sources.yaml.
* data_health (Workshop 4, user story US4): per source, records in Silver, rejected records, time
  of the newest Bronze file and watermark age (newest source timestamp stored in Silver).

Dashboard summary (Workshop 4):
* kpi_summary: one headline value per KPI (metric names what it is, e.g. mean_pct_null), its
  threshold_max / threshold_min (config/sources.yaml, governance.thresholds) and status 1 pass / 0 warn. It is computed from the Gold rows of the other
  indicators, after them, so the KPI cards and the quality table show exactly what the charts show.

Not included: language_distribution (single-language corpus, finding F22).

Two inputs are not Silver rows: duplicate_rate reads the ids of the FDSN Bronze files (the only way
to see duplicates that Silver already removed), and schema_compliance reads each Silver file's
schema. freshness and ingestion_frequency use the bronze_file lineage column, so Bronze files that
added no row to Silver (finding F11) are not seen.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from functools import reduce
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import url2pathname

log = logging.getLogger(__name__)

BRONZE_NAME = r"^(.+)_(\d{8}_\d{6})\.json$"          # <source_id>_YYYYMMDD_HHMMSS.json
EVENT_TIME = {"earthquakes": "event_time", "notices": "sent_time"}
REASON_CATEGORY = {
    "non_tectonic_type": "business_rule",      # quarry blasts, explosions: out of scope by design
    "no_notice_issued": "business_rule",       # volcano without a HANS notice
    "region_level_notice": "business_rule",    # region-wide notice, not a volcano alert
    "missing_id_or_time": "source_defect",     # the source sent a record that cannot be keyed
    "unknown_source": "pipeline_error",        # our bug (reference file quarantined, fixed 2026-11-19)
}
OUTLIER_FLAGS = {"mag": "is_outlier_mag_iqr", "depth_km": "is_outlier_depth_iqr"}
MIN_EVENTS_PER_VOLCANO = 30
IQR_K = 1.5
MIN_FRESH_HOURS = 24.0   # freshness KPI: no source may stay silent for more than a day
MOJIBAKE = "[âäåœïÃ€™]"   # bytes of UTF-8 text decoded as cp1252 / Latin-1 (finding F21)
RUNS_PER_DAY = {"0 * * * *": 24.0, "@hourly": 24.0, "*/15 * * * *": 24.0,   # bronze_ingestion is hourly
                "0 */6 * * *": 4.0, "@daily": 1.0, "@monthly": 1 / 30}


# ---------------------------------------------------------------------------------------- helpers

def _union(parts: list):
    parts = [p for p in parts if p is not None]
    return reduce(lambda a, b: a.unionByName(b), parts) if parts else None


def _latest(df):
    from .silver_reader import latest

    return latest(df)


def _data_topics(silver: dict) -> dict:
    return {t: df for t, df in silver.items() if df is not None and t != "quarantine"}


def _source(col: str = "bronze_file"):
    from pyspark.sql import functions as F

    return F.regexp_extract(col, BRONZE_NAME, 1)


def _stamp(col: str = "bronze_file"):
    from pyspark.sql import functions as F

    return F.to_timestamp(F.regexp_extract(col, BRONZE_NAME, 2), "yyyyMMdd_HHmmss")


def _lineage(silver: dict):
    """Distinct Bronze files that produced at least one Silver or quarantine row."""
    files = [df.select("bronze_file") for df in silver.values() if df is not None and "bronze_file" in df.columns]
    files = _union(files)
    return files.where("bronze_file IS NOT NULL").distinct() if files is not None else None


def _lake_root(silver: dict) -> Path | None:
    """Folder holding datalake_silver/ and datalake_bronze/, from the files Spark read."""
    for df in silver.values():
        files = df.inputFiles() if df is not None else []
        if files:
            url = urlparse(files[0])
            if url.scheme in ("", "file"):
                return Path(url2pathname(url.path)).parents[2]   # <root>/datalake_silver/<topic>/<file>
    return None


def _rows(df, section: str, unit: str):
    """Add the constant contract columns to a DataFrame that has metric, value and dimension."""
    from pyspark.sql import functions as F

    cols = ["metric", "dimension", "value"] + (["period"] if "period" in df.columns else [])
    return df.select(F.lit(section).alias("section"), *cols, F.lit(unit).alias("unit"))


# ------------------------------------------------------------------------------------ volume (ops)

def volume_total(spark, silver: dict):
    rows = [(topic, float(df.count())) for topic, df in silver.items() if df is not None]
    if not rows:
        return None
    return spark.createDataFrame(rows, "dimension string, value double").selectExpr(
        "'volume_total' AS section", "'records' AS metric", "dimension", "value", "'records' AS unit")


def volume_weekly(spark, silver: dict):
    """Records per week (newest version) of the topics with an event time: earthquakes, notices.

    metric records: per topic; metric records_by_source (Workshop 4, volume chart of the governance
    dashboard): per Bronze source, e.g. earthquakes split into usgs_feed and usgs_fdsn.
    """
    from pyspark.sql import functions as F

    parts = []
    for topic, col in EVENT_TIME.items():
        df = silver.get(topic)
        if df is None:
            continue
        current = (_latest(df).where(F.col(col).isNotNull())
                   .select(F.date_trunc("week", col).alias("period"), _source().alias("source")))
        by_topic = current.groupBy("period").count().select(
            F.lit("records").alias("metric"), F.lit(topic).alias("dimension"), "period",
            F.col("count").cast("double").alias("value"))
        by_source = current.groupBy("period", "source").count().select(
            F.lit("records_by_source").alias("metric"), F.col("source").alias("dimension"), "period",
            F.col("count").cast("double").alias("value"))
        parts += [_rows(by_topic, "volume_weekly", "records"), _rows(by_source, "volume_weekly", "records")]
    return _union(parts)


# ----------------------------------------------------------------------------- data-quality KPIs

def null_rate(spark, silver: dict):
    """% missing per field: null, blank string or empty list (findings F01-F05)."""
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    parts = []
    for topic, df in _data_topics(silver).items():
        current = _latest(df)
        exprs = []
        for field in current.schema:
            col = F.col(f"`{field.name}`")
            missing = col.isNull()
            if isinstance(field.dataType, T.StringType):
                missing = missing | (F.trim(col) == "")
            elif isinstance(field.dataType, T.ArrayType):
                missing = missing | (F.size(col) == 0)
            exprs.append((F.avg(missing.cast("double")) * 100).alias(field.name))
        long = current.agg(*exprs).unpivot([], current.columns, "column", "value")
        parts.append(_rows(long.select(F.lit("pct_null").alias("metric"),
                                       F.concat(F.lit(f"{topic}."), "column").alias("dimension"), "value"),
                           "null_rate", "%"))
    return _union(parts)


def _fdsn_before_dedup(spark, silver: dict):
    """Repeated event ids among the FDSN Bronze files already processed by Silver (finding F09)."""
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    root = _lake_root(silver)
    if root is None:
        return None
    files = sorted((root / "datalake_bronze").glob("usgs_fdsn_*.json"))
    manifest = root / "datalake_silver" / "_state" / "manifest.json"
    if manifest.exists():                      # skip files that Silver has not processed yet
        done = set(json.loads(manifest.read_text()))
        files = [f for f in files if f.name in done]
    if not files:
        return None
    # explicit schema: Spark parses only the ids instead of inferring the whole GeoJSON
    schema = T.StructType([T.StructField("payload", T.StructType([
        T.StructField("features", T.ArrayType(T.StructType([T.StructField("id", T.StringType())])))]))])
    ids = (spark.read.schema(schema).option("multiLine", True).json([str(f) for f in files])
           .select(F.explode("payload.features.id").alias("id")))
    stats = ids.agg(F.count("*").alias("n"), F.countDistinct("id").alias("d"))
    return stats.select(F.lit("usgs_fdsn").alias("dimension"), F.expr(
        "stack(3, 'records_before_dedup', CAST(n AS DOUBLE), 'records', "
        "'repeated_before_dedup', CAST(n - d AS DOUBLE), 'records', "
        "'pct_repeated_before_dedup', CAST(100 * (n - d) / n AS DOUBLE), '%') AS (metric, value, unit)"))


def _value_columns(df) -> list[str]:
    """Columns that describe the record itself: no key, hash, lineage or per-batch outlier flag."""
    skip = {"record_id", "content_hash", "bronze_file", "bronze_sha256", "processed_utc"}
    return [c for c in df.columns if c not in skip and not c.startswith("is_outlier_")]


def duplicate_rate(spark, silver: dict):
    """After dedup per Silver topic (F06-F08, F10) and before dedup on FDSN Bronze (F09, F11).

    The extra rows of a record_id are split in three, all as % of the topic rows:
    * pct_exact_copies: same record_id and same content_hash (the dedup let a copy through);
    * pct_same_value_copies: same record_id and the same values, whatever the hash (includes the
      exact copies; the rest are copies whose hash changed although no value did);
    * pct_real_revisions: same record_id with at least one value changed (legitimate versions).
    pct_same_value_copies + pct_real_revisions = all extra rows.
    """
    from pyspark.sql import functions as F

    parts = []
    for topic, df in _data_topics(silver).items():
        value_cols = _value_columns(df)
        values_hash = F.xxhash64(*[F.col(f"`{c}`") for c in value_cols]) if value_cols else F.lit(0)
        stats = df.agg(F.count("*").alias("n"), F.countDistinct("record_id").alias("ids"),
                       F.countDistinct("record_id", "content_hash").alias("pairs"),
                       F.countDistinct("record_id", values_hash).alias("versions"))
        parts.append(stats.select(F.lit(topic).alias("dimension"), F.expr(
            "stack(3, 'pct_exact_copies', CAST(100 * (n - pairs) / n AS DOUBLE), "
            "'pct_same_value_copies', CAST(100 * (n - versions) / n AS DOUBLE), "
            "'pct_real_revisions', CAST(100 * (versions - ids) / n AS DOUBLE)) AS (metric, value)"),
            F.lit("%").alias("unit")))
    try:
        parts.append(_fdsn_before_dedup(spark, silver))
    except Exception as exc:   # Bronze not mounted or unreadable: keep the Silver part of the KPI
        log.warning("duplicate_rate: FDSN before-dedup part skipped (%s)", exc)
    out = _union(parts)
    return out.select(F.lit("duplicate_rate").alias("section"), "metric", "dimension", "value", "unit") if out else None


def _contract_type(t) -> str:
    """Spark type name of a pyarrow contract type (dictionary columns are read as strings)."""
    import pyarrow as pa

    if pa.types.is_dictionary(t):
        return _contract_type(t.value_type)
    if pa.types.is_timestamp(t):
        return "timestamp"          # the unit is not compared: Silver writes us on purpose for Spark
    if pa.types.is_list(t):
        return f"array<{_contract_type(t.value_type)}>"
    return {"int64": "bigint", "bool": "boolean"}.get(str(t), str(t))


def schema_compliance(spark, silver: dict):
    """% of Silver files whose columns and types are exactly the contract (finding F12)."""
    from pyspark.sql import functions as F

    from ..silver.schemas import QUARANTINE_SCHEMA, SCHEMAS

    contracts = {**SCHEMAS, "quarantine": QUARANTINE_SCHEMA}
    rows = []
    for topic, df in silver.items():
        if df is None or topic not in contracts:
            continue
        expected = [(f.name, _contract_type(f.type)) for f in contracts[topic]]
        files = df.inputFiles()
        if not files:
            continue
        ok = sum([(f.name, f.dataType.simpleString()) for f in spark.read.parquet(p).schema] == expected
                 for p in files)
        rows.append((topic, 100.0 * ok / len(files), float(len(files))))
    if not rows:
        return None
    return (spark.createDataFrame(rows, "dimension string, pct double, files double")
            .select(F.lit("schema_compliance").alias("section"), "dimension", F.expr(
                "stack(2, 'pct_files_conforming', pct, '%', 'files', files, 'files') AS (metric, value, unit)")))


def quarantine_rate(spark, silver: dict):
    """Quarantined / processed records per source, with reasons as breakdown (findings F13-F16)."""
    from pyspark.sql import functions as F

    data = _union([df.select(_source().alias("source")) for df in _data_topics(silver).values()])
    q = silver.get("quarantine")
    if data is None and q is None:
        return None
    in_silver = (data.groupBy("source").agg(F.count("*").alias("in_silver")) if data is not None
                 else spark.createDataFrame([], "source string, in_silver long"))
    category = F.create_map(*[F.lit(x) for kv in REASON_CATEGORY.items() for x in kv])
    if q is not None:
        rejected = q.select(F.col("source_id").alias("source"), "reason",
                            F.coalesce(category[F.col("reason")], F.lit("unclassified")).alias("category"))
        quarantined = rejected.groupBy("source").agg(F.count("*").alias("quarantined"))
        by_reason = (rejected.groupBy("source", "reason", "category").count()
                     .select(F.lit("records_by_reason").alias("metric"),
                             F.concat_ws("|", "source", "reason", "category").alias("dimension"),
                             F.col("count").cast("double").alias("value"), F.lit("records").alias("unit")))
    else:
        quarantined = spark.createDataFrame([], "source string, quarantined long")
        by_reason = None
    per_source = in_silver.join(quarantined, "source", "full").fillna(0, ["in_silver", "quarantined"])
    per_source = per_source.unionByName(per_source.agg(
        F.lit("all sources").alias("source"), F.sum("in_silver").alias("in_silver"),
        F.sum("quarantined").alias("quarantined")))
    rates = per_source.select(
        F.lit("pct_quarantined").alias("metric"), F.col("source").alias("dimension"),
        (100 * F.col("quarantined") / (F.col("in_silver") + F.col("quarantined"))).cast("double").alias("value"),
        F.lit("%").alias("unit"))
    out = _union([rates, by_reason])
    return out.select(F.lit("quarantine_rate").alias("section"), "metric", "dimension", "value", "unit")


def outlier_rate(spark, silver: dict):
    """IQR outliers with per-volcano fences over all records (findings F17-F19)."""
    from pyspark.sql import functions as F

    eq = silver.get("earthquakes")
    if eq is None:
        return None
    eq = _latest(eq)
    parts = []
    for col, flag in OUTLIER_FLAGS.items():
        values = eq.where(F.col(col).isNotNull())
        overall = values.agg(F.percentile_approx(col, [0.25, 0.75]).alias("q")).select(
            F.col("q")[0].alias("g_q1"), F.col("q")[1].alias("g_q3"))
        per_volcano = values.groupBy("nearest_vnum").agg(
            F.percentile_approx(col, [0.25, 0.75]).alias("q"), F.count("*").alias("n")).select(
            "nearest_vnum", F.col("q")[0].alias("v_q1"), F.col("q")[1].alias("v_q3"), "n")
        own = F.coalesce(F.col("n") >= MIN_EVENTS_PER_VOLCANO, F.lit(False))   # null vnum -> global
        q1 = F.when(own, F.col("v_q1")).otherwise(F.col("g_q1"))
        q3 = F.when(own, F.col("v_q3")).otherwise(F.col("g_q3"))
        is_out = (F.col(col) < q1 - IQR_K * (q3 - q1)) | (F.col(col) > q3 + IQR_K * (q3 - q1))
        stats = (values.join(per_volcano, "nearest_vnum", "left").crossJoin(overall)
                 .agg((F.avg(is_out.cast("double")) * 100).alias("per_volcano"),
                      (F.avg(F.col(flag).cast("double")) * 100).alias("silver_flags")))
        parts.append(stats.select(F.lit(f"earthquakes.{col}").alias("dimension"), F.expr(
            "stack(2, 'pct_outlier', per_volcano, 'pct_outlier_silver_flags', silver_flags) AS (metric, value)")))
    return _rows(_union(parts), "outlier_rate", "%")


def text_length(spark, silver: dict):
    """Characters per notice by observatory (all-time and per year) and the encoding-error rate (F20-F22)."""
    from pyspark.sql import functions as F

    def stats(length):
        return [F.avg(length).cast("double").alias("mean_chars"),
                F.percentile_approx(length, 0.5).cast("double").alias("median_chars"),
                F.min(length).cast("double").alias("min_chars"),
                F.max(length).cast("double").alias("max_chars")]

    metrics = ["mean_chars", "median_chars", "min_chars", "max_chars"]
    parts = []
    notices = silver.get("notices")
    if notices is not None:
        cur = _latest(notices)
        length = F.col("text_length")
        by_year = cur.groupBy(F.col("observatory").alias("dimension"),
                              F.trunc(F.to_date("sent_time"), "year").alias("period")).agg(*stats(length))
        by_obs = (cur.groupBy(F.col("observatory").alias("dimension")).agg(*stats(length))
                  .withColumn("period", F.lit(None).cast("date")))
        overall = (cur.agg(*stats(length)).withColumn("dimension", F.lit("notices"))
                   .withColumn("period", F.lit(None).cast("date")))
        for df in (by_year, by_obs, overall):
            parts.append(df.unpivot(["dimension", "period"], metrics, "metric", "value")
                         .withColumn("unit", F.lit("chars")))
        encoding = cur.agg((F.avg(F.coalesce(F.col("text_clean").rlike(MOJIBAKE), F.lit(False)).cast("double"))
                            * 100).alias("value"))
        parts.append(encoding.select(F.lit("notices").alias("dimension"), F.lit(None).cast("date").alias("period"),
                                     F.lit("pct_encoding_errors").alias("metric"), "value", F.lit("%").alias("unit")))
    updates = silver.get("volcano_updates")
    if updates is not None:
        upd = (_latest(updates).agg(*stats(F.length("text_clean"))).withColumn("dimension", F.lit("volcano_updates"))
               .withColumn("period", F.lit(None).cast("date")))
        parts.append(upd.unpivot(["dimension", "period"], metrics, "metric", "value").withColumn("unit", F.lit("chars")))
    out = _union(parts)
    return out.select(F.lit("text_length").alias("section"), "metric", "dimension", "period", "value", "unit") \
        if out is not None else None


# --------------------------------------------------------------------------- timeliness (ops)

def freshness(spark, silver: dict):
    """Hours since the newest Bronze file that reached Silver, per source (finding F24)."""
    from pyspark.sql import functions as F

    files = _lineage(silver)
    if files is None:
        return None
    last = files.groupBy(_source().alias("dimension")).agg(F.max(_stamp()).alias("last"))
    hours = (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp("last")) / 3600
    return last.select(F.lit("freshness").alias("section"), F.lit("hours_since_last").alias("metric"),
                       "dimension", hours.cast("double").alias("value"), F.lit("hours").alias("unit"))


def _runs_per_day() -> dict[str, float]:
    from ..config import load_config

    try:
        sources = load_config()["sources"]
    except Exception as exc:   # no config: the KPI still reports hours_with_file
        log.warning("ingestion_frequency: sources.yaml not readable (%s)", exc)
        return {}
    return {s["source_id"]: RUNS_PER_DAY[s["schedule"]] for s in sources
            if s.get("enabled", True) and s.get("schedule") in RUNS_PER_DAY}


def ingestion_frequency(spark, silver: dict):
    """Hours with a new Bronze file per source and day vs the schedule (finding F23).

    Bronze writes a file only when the source changed, so a backfill of 200 files in one hour
    counts once and a quiet hour without a file is not necessarily a failure.
    """
    from pyspark.sql import functions as F

    files = _lineage(silver)
    if files is None:
        return None
    expected = _runs_per_day()
    per_day = (files.select(_source().alias("dimension"), _stamp().alias("stamp"))
               .groupBy("dimension", F.to_date("stamp").alias("period"))
               .agg(F.countDistinct(F.date_trunc("hour", "stamp")).cast("double").alias("hours_with_file")))
    exp = (F.create_map(*[F.lit(x) for kv in expected.items() for x in kv])[F.col("dimension")]
           if expected else F.lit(None).cast("double"))
    per_day = per_day.withColumn("expected_per_day", exp.cast("double")).withColumn(
        "pct_of_expected", F.least(F.lit(100.0), 100 * F.col("hours_with_file") / F.col("expected_per_day")))
    return per_day.select(F.lit("ingestion_frequency").alias("section"), "dimension", "period", F.expr(
        "stack(3, 'hours_with_file', hours_with_file, 'hours', 'expected_per_day', expected_per_day, 'runs', "
        "'pct_of_expected', pct_of_expected, '%') AS (metric, value, unit)"))


# newest source timestamp of each topic: the watermark the incremental extraction advances
WATERMARK_COLUMN = {"earthquakes": "updated_time", "alert_notices": "sent_time", "notices": "sent_time",
                    "volcano_updates": "as_of_time"}


def data_health(spark, silver: dict):
    """Per source: records in Silver, rejected records, newest file and watermark age (US4)."""
    from pyspark.sql import functions as F

    parts = []
    data = _union([df.select(_source().alias("dimension")) for df in _data_topics(silver).values()])
    if data is not None:
        parts.append(data.groupBy("dimension").count().select(
            "dimension", F.lit("records_in_silver").alias("metric"),
            F.col("count").cast("double").alias("value"), F.lit("records").alias("unit")))
    q = silver.get("quarantine")
    if q is not None:
        parts.append(q.groupBy(F.col("source_id").alias("dimension")).count().select(
            "dimension", F.lit("rejected_records").alias("metric"),
            F.col("count").cast("double").alias("value"), F.lit("records").alias("unit")))
    files = _lineage(silver)
    if files is not None:
        # epoch seconds, so the value fits the double column; the dashboard formats it as a date
        parts.append(files.groupBy(_source().alias("dimension")).agg(F.max(_stamp()).alias("last")).select(
            "dimension", F.lit("last_file_utc").alias("metric"),
            F.unix_timestamp("last").cast("double").alias("value"), F.lit("unix_s").alias("unit")))
    marks = _union([silver[t].select(_source().alias("dimension"), F.col(col).alias("mark"))
                    for t, col in WATERMARK_COLUMN.items() if silver.get(t) is not None])
    if marks is not None:
        age = (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.max("mark"))) / 3600
        parts.append(marks.groupBy("dimension").agg(age.cast("double").alias("value")).select(
            "dimension", F.lit("watermark_age_hours").alias("metric"), "value", F.lit("hours").alias("unit")))
    out = _union(parts)
    return out.select(F.lit("data_health").alias("section"), "metric", "dimension", "value", "unit") \
        if out is not None else None


KPIS = [volume_total, volume_weekly, null_rate, duplicate_rate, schema_compliance, quarantine_rate,
        outlier_rate, text_length, freshness, ingestion_frequency, data_health]


# ------------------------------------------------------------------------- kpi_summary (W4)

def _thresholds() -> dict:
    from ..config import load_config

    try:
        return load_config().get("governance", {}).get("thresholds", {}) or {}
    except Exception as exc:   # no config: the summary still shows the values, without status
        log.warning("kpi_summary: thresholds not readable (%s)", exc)
        return {}


def _headlines(spark, rows):
    """One headline value per KPI, computed from the Gold rows of the other indicators.

    Returns dimension (KPI name), metric (what the headline is), value and unit.
    """
    from pyspark.sql import functions as F

    def pick(section, metric=None, dimension=None):
        df = rows.where(F.col("section") == section)
        if metric is not None:
            df = df.where(F.col("metric") == metric)
        if dimension is not None:
            df = df.where(F.col("dimension") == dimension)
        return df

    def one(kpi, metric, df, value, unit):
        return df.agg(value.cast("double").alias("value")).select(
            F.lit(kpi).alias("dimension"), F.lit(metric).alias("metric"), "value", F.lit(unit).alias("unit"))

    generated = rows.agg(F.max("generated_utc").alias("g"))
    # exact copies weighted by the rows of each topic (volume_total), over all Silver topics
    copies = (pick("duplicate_rate", "pct_exact_copies").alias("d")
              .join(pick("volume_total").alias("v"), "dimension")
              .select((F.col("d.value") * F.col("v.value")).alias("copies"), F.col("v.value").alias("n")))
    # freshness: a source is fresh when its newest file is younger than a day, or than twice its
    # schedule interval when that is longer (monthly GVP). Not 2 x 1 h for the hourly sources: Bronze
    # writes a file only when the source changed (F23) and HANS does not publish every hour; the
    # hourly feed's gaps are what ingestion_frequency checks.
    max_age = {s: max(MIN_FRESH_HOURS, 2 * 24 / per_day) for s, per_day in _runs_per_day().items()}
    limit = F.create_map(*[F.lit(x) for kv in max_age.items() for x in kv])[F.col("dimension")] if max_age \
        else F.lit(None).cast("double")
    fresh = pick("freshness", "hours_since_last").withColumn("limit", limit).where("limit IS NOT NULL")
    # ingestion: hours with a new usgs_feed file over the last 7 full days (the feed changes every hour)
    feed = (pick("ingestion_frequency", "hours_with_file", "usgs_feed").crossJoin(generated)
            .where((F.col("period") >= F.date_sub(F.to_date("g"), 7)) & (F.col("period") < F.to_date("g"))))
    return _union([
        one("volume", "records_in_silver", pick("volume_total").where("dimension != 'quarantine'"),
            F.sum("value"), "records"),
        one("null_rate", "mean_pct_null", pick("null_rate", "pct_null"), F.avg("value"), "%"),
        one("duplicate_rate", "pct_exact_copies", copies, F.sum("copies") / F.sum("n"), "%"),
        one("schema_compliance", "min_pct_files_conforming", pick("schema_compliance", "pct_files_conforming"),
            F.min("value"), "%"),
        one("quarantine_rate", "pct_quarantined", pick("quarantine_rate", "pct_quarantined", "all sources"),
            F.max("value"), "%"),
        one("outlier_rate", "max_pct_outlier", pick("outlier_rate", "pct_outlier"), F.max("value"), "%"),
        one("text_length", "pct_encoding_errors", pick("text_length", "pct_encoding_errors", "notices"),
            F.max("value"), "%"),
        one("freshness", "pct_sources_fresh", fresh,
            100 * F.avg((F.col("value") <= F.col("limit")).cast("double")), "%"),
        one("ingestion_frequency", "feed_pct_hours_7d", feed,
            100 * F.coalesce(F.sum("value"), F.lit(0.0)) / (7 * 24), "%"),
    ])


def kpi_summary(spark, rows, thresholds: dict | None = None):
    """Per KPI: headline value, threshold_max / threshold_min and status (1 pass, 0 warn).

    KPIs without a threshold (volume) get only the value; status rows are written only when both
    the value and the threshold exist.
    """
    from pyspark.sql import functions as F

    thresholds = _thresholds() if thresholds is None else thresholds
    head = _headlines(spark, rows)
    limits = spark.createDataFrame([(k, float(t["value"]), str(t["rule"])) for k, t in thresholds.items()],
                                   "dimension string, threshold double, rule string")
    joined = head.join(limits, "dimension").where("value IS NOT NULL")
    passed = F.when(F.col("rule") == "max", F.col("value") <= F.col("threshold")) \
        .otherwise(F.col("value") >= F.col("threshold"))
    threshold_rows = joined.select("dimension", F.concat(F.lit("threshold_"), "rule").alias("metric"),
                                   F.col("threshold").alias("value"), "unit")
    status_rows = joined.select("dimension", F.lit("status").alias("metric"),
                                passed.cast("double").alias("value"), F.lit("pass=1").alias("unit"))
    return _union([head, threshold_rows, status_rows]).select(
        F.lit("kpi_summary").alias("section"), "metric", "dimension", "value", "unit")


def build_governance(spark, silver: dict, generated: datetime):
    from .contract import conform, union_all

    parts = [kpi(spark, silver) for kpi in KPIS]
    # localCheckpoint materialises the ~2,500 rows and cuts their lineage: kpi_summary reads them
    # about ten times, and with the full plan of eleven indicators behind each read Spark spent
    # minutes re-analysing the query tree (Silver samples test: 142 s before, 22 s after)
    rows = union_all(spark, [conform(p, generated) for p in parts if p is not None]).localCheckpoint()
    return rows.unionByName(conform(kpi_summary(spark, rows), generated))
