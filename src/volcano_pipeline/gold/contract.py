"""Gold output contract shared by the governance and storytelling summaries.

Both summaries are one long table, one row per number shown in a dashboard:

    section        chart or card the row feeds (e.g. null_rate, volume_weekly, tone_weekly)
    metric         what is measured (e.g. pct_null, records, mean_tone)
    dimension      breakdown value: field, source, volcano, keyword...; null if none
    period         start of the period (week or day, UTC); null for all-time values
    value          the number
    unit           %, records, score, km...
    generated_utc  when the Gold run produced the row

A builder returns any DataFrame with section, metric, value and optionally dimension, period and
unit; conform() adds the missing columns, casts the types and stamps generated_utc. The Workshop 4
dashboards filter this table by section, so every new aggregation only needs a new section name.
"""
from __future__ import annotations

import shutil
from datetime import datetime, timezone
from pathlib import Path

COLUMNS = ["section", "metric", "dimension", "period", "value", "unit", "generated_utc"]
REQUIRED = {"section", "metric", "value"}


def gold_schema():
    from pyspark.sql import types as T

    return T.StructType([
        T.StructField("section", T.StringType(), False),
        T.StructField("metric", T.StringType(), False),
        T.StructField("dimension", T.StringType(), True),
        T.StructField("period", T.DateType(), True),
        T.StructField("value", T.DoubleType(), True),
        T.StructField("unit", T.StringType(), True),
        T.StructField("generated_utc", T.TimestampType(), False),
    ])


def conform(df, generated: datetime):
    """Return df with exactly the contract columns, in order and with the contract types."""
    from pyspark.sql import functions as F

    missing = REQUIRED - set(df.columns)
    if missing:
        raise ValueError(f"Gold rows need columns {sorted(missing)}")
    cols = {
        "section": F.col("section").cast("string"),
        "metric": F.col("metric").cast("string"),
        "dimension": F.col("dimension").cast("string") if "dimension" in df.columns else F.lit(None).cast("string"),
        "period": F.to_date(F.col("period")) if "period" in df.columns else F.lit(None).cast("date"),
        "value": F.col("value").cast("double"),
        "unit": F.col("unit").cast("string") if "unit" in df.columns else F.lit(None).cast("string"),
        # keep the datetime aware: PySpark reads a naive datetime as local time, which shifted the
        # stamp by the host's UTC offset outside Docker (e.g. +5 h in Colombia)
        "generated_utc": F.lit(generated.astimezone(timezone.utc)).cast("timestamp"),
    }
    return df.select(*[cols[c].alias(c) for c in COLUMNS])


def empty(spark):
    return spark.createDataFrame([], gold_schema())


def union_all(spark, parts: list):
    out = empty(spark)
    for part in parts:
        out = out.unionByName(part)
    return out


def gold_name(kind: str, when: datetime) -> str:
    return f"{kind}_{when.astimezone(timezone.utc):%Y%m%d_%H%M%S}.parquet"


def write_summary(df, gold_dir: Path, kind: str, when: datetime) -> Path:
    """Write df as one Parquet file datalake_gold/<kind>_YYYYMMDD_HHMMSS.parquet.

    Spark always writes a folder of part files. The summaries are small, so they are coalesced to
    one partition, written to a temporary folder and the single part file is moved to the final
    name, which pandas (Workshop 4 dashboards) reads directly.
    """
    gold_dir.mkdir(parents=True, exist_ok=True)
    target = gold_dir / gold_name(kind, when)
    tmp = gold_dir / f".{target.stem}.tmp"
    shutil.rmtree(tmp, ignore_errors=True)
    df.coalesce(1).write.mode("overwrite").parquet(str(tmp))
    [part] = list(tmp.glob("part-*.parquet"))
    part.replace(target)
    shutil.rmtree(tmp, ignore_errors=True)
    return target
