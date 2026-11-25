"""Read the Silver layer with PySpark, one topic folder at a time.

spark.read.parquet(<folder>) reads every Parquet file of a folder as one DataFrame, the same call
that reads an S3 or Azure Data Lake prefix in production. Each Silver topic has its own folder
(datalake_silver/<topic>/), so all the files read together share one schema. The whole
datalake_silver/ folder is not read at once: it mixes six schemas plus the _state/ files.
"""
from __future__ import annotations

from pathlib import Path

TOPICS = ["volcanoes", "eruptions", "earthquakes", "alert_notices", "volcano_updates", "notices"]


def topic_dir(silver_dir: Path, topic: str) -> Path:
    return silver_dir / ("quarantine" if topic == "quarantine" else topic)


def read_topic(spark, silver_dir: Path, topic: str):
    """All Silver files of one topic as one DataFrame, or None if the topic has no file yet.

    mergeSchema: a folder can mix files written before and after a column was added (Workshop 4
    added end_time / is_continuing to eruptions). Without it Spark takes the schema of one file and
    the new columns can silently disappear."""
    folder = topic_dir(silver_dir, topic)
    if not any(folder.glob("*.parquet")):
        return None
    return spark.read.option("mergeSchema", "true").parquet(str(folder))


def latest(df):
    """Keep the newest version of each record. Silver writes a record again when it changes at the
    source (a revised magnitude, the feed repeating its week), so a topic folder can hold several
    versions of one record_id; count records only after this."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    w = Window.partitionBy("record_id").orderBy(F.col("processed_utc").desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def read_silver(spark, silver_dir: Path) -> dict:
    """{topic: DataFrame or None} for the five topics and the quarantine."""
    return {t: read_topic(spark, silver_dir, t) for t in TOPICS + ["quarantine"]}
