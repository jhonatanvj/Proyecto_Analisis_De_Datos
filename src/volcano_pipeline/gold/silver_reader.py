"""Read the Silver layer with PySpark, one topic folder at a time.

spark.read.parquet(<folder>) reads every Parquet file of a folder as one DataFrame, the same call
that reads an S3 or Azure Data Lake prefix in production. Each Silver topic has its own folder
(datalake_silver/<topic>/), so all the files read together share one schema. The whole
datalake_silver/ folder is not read at once: it mixes five schemas plus the _state/ files.
"""
from __future__ import annotations

from pathlib import Path

TOPICS = ["volcanoes", "eruptions", "earthquakes", "alert_notices", "volcano_updates"]


def topic_dir(silver_dir: Path, topic: str) -> Path:
    return silver_dir / ("quarantine" if topic == "quarantine" else topic)


def read_topic(spark, silver_dir: Path, topic: str):
    """All Silver files of one topic as one DataFrame, or None if the topic has no file yet."""
    folder = topic_dir(silver_dir, topic)
    if not any(folder.glob("*.parquet")):
        return None
    return spark.read.parquet(str(folder))


def read_silver(spark, silver_dir: Path) -> dict:
    """{topic: DataFrame or None} for the five topics and the quarantine."""
    return {t: read_topic(spark, silver_dir, t) for t in TOPICS + ["quarantine"]}
