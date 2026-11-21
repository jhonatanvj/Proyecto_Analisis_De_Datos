"""Build one Gold summary: Silver folders -> PySpark -> datalake_gold/<kind>_YYYYMMDD_HHMMSS.parquet.

Used by the gold_processing DAG (one task per summary) and by scripts/run_pipeline_local.py.
Each call starts and stops its own SparkSession, so the two tasks never share JVM memory.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from .governance import build_governance
from .storytelling import build_storytelling

log = logging.getLogger(__name__)

BUILDERS = {"governance": build_governance, "storytelling": build_storytelling}


def run_summary(kind: str, silver_dir: Path, gold_dir: Path, when: datetime | None = None, spark=None) -> dict:
    from .contract import write_summary
    from .silver_reader import read_silver
    from .spark import get_spark

    when = (when or datetime.now(timezone.utc)).astimezone(timezone.utc)
    own = spark is None
    spark = spark or get_spark(f"volcano-gold-{kind}")
    try:
        silver = read_silver(spark, silver_dir)
        df = BUILDERS[kind](spark, silver, when).cache()
        rows = df.count()
        path = write_summary(df, gold_dir, kind, when)
        sections = sorted(r["section"] for r in df.select("section").distinct().collect())
    finally:
        if own:
            spark.stop()
    summary = {"kind": kind, "file": path.name, "rows": rows, "sections": sections,
               "silver_topics": sorted(t for t, d in silver.items() if d is not None)}
    log.info(json.dumps({"event": "gold_summary", **summary}))
    return summary
