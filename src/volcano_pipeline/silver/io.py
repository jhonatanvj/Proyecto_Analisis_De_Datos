"""Write Silver topic files and the quarantine file, named <name>_YYYYMMDD_HHMMSS.parquet (UTC).

Each topic has its own folder (datalake_silver/<topic>/), so the Gold DAG can read one topic as
a single DataFrame with spark.read.parquet("datalake_silver/<topic>/"): every file in that folder
has the same schema. Rejected records go to datalake_silver/quarantine/.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from .schemas import QUARANTINE_SCHEMA, SCHEMAS, enforce, to_table


def _name(prefix: str, when: datetime) -> str:
    return f"{prefix}_{when.astimezone(timezone.utc):%Y%m%d_%H%M%S}.parquet"


def topic_path(silver_dir: Path, topic: str, when: datetime) -> Path:
    return silver_dir / topic / _name(topic, when)


def _write(table, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".parquet.part")
    # microseconds: Spark 3 cannot read Parquet TIMESTAMP(NANOS) and Workshop 3 reads this folder with PySpark
    pq.write_table(table, tmp, coerce_timestamps="us", allow_truncated_timestamps=True)
    tmp.replace(path)
    return path


def write_topic(df: pd.DataFrame, silver_dir: Path, topic: str, when: datetime) -> Path | None:
    if df.empty:
        return None
    table = to_table(enforce(df, topic), SCHEMAS[topic])
    return _write(table, topic_path(silver_dir, topic, when))


def write_quarantine(rows: list[dict], silver_dir: Path, when: datetime) -> Path | None:
    if not rows:
        return None
    df = pd.DataFrame([{
        "source_id": r.get("source_id"),
        "reason": r.get("reason"),
        "payload": r["payload"] if isinstance(r.get("payload"), str)
        else json.dumps(r.get("payload"), ensure_ascii=False, default=str),
        "bronze_file": r.get("bronze_file"),
        "processed_utc": r.get("processed_utc", when),
    } for r in rows])
    table = to_table(enforce(df, "quarantine", QUARANTINE_SCHEMA), QUARANTINE_SCHEMA)
    return _write(table, silver_dir / "quarantine" / _name("rejected", when))
