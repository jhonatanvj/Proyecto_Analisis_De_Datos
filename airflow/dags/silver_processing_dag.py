"""Silver processing: clean the new Bronze files, write Parquet files to datalake_silver/ and
load the new Silver files into the project database (PostgreSQL + PostGIS, service project-db).

A FileSensor watches datalake_bronze/*.json. The plain FileSensor would succeed as soon as any
Bronze file exists, including files already processed, so NewBronzeFileSensor also checks the
Silver manifest and only succeeds when at least one file is new.

Timing: Bronze runs at minute 0 of every hour; this DAG starts at minute 15 and pokes every
60 s for up to 40 min (mode=reschedule frees the worker slot between pokes). If no new file
shows up the run ends as skipped (soft_fail), not failed, because "nothing new this hour" is a
normal outcome when the sources did not change.

load_to_database loads every Silver file not yet in the database (see
volcano_pipeline/storage/loader.py): one transaction per file, upserts by content hash, run log
and watermark in the same transaction. If the database is down the task fails and retries; the
files stay pending and the next run loads them.
"""
from __future__ import annotations

import json
import logging
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor

from volcano_pipeline.config import datalake_dir, load_config
from volcano_pipeline.silver.run import process_bronze_files, unprocessed_files
from volcano_pipeline.storage.loader import load_pending

log = logging.getLogger("silver_processing")


class NewBronzeFileSensor(FileSensor):
    """FileSensor that ignores Bronze files already listed in the Silver manifest."""

    def poke(self, context) -> bool:
        if not super().poke(context):
            return False
        pending = unprocessed_files(datalake_dir("bronze"), datalake_dir("silver"))
        self.log.info("Bronze files waiting for Silver: %d", len(pending))
        return bool(pending)


@dag(
    dag_id="silver_processing",
    schedule="15 * * * *",
    start_date=pendulum.datetime(2026, 11, 16, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["volcano", "silver"],
    doc_md=__doc__,
)
def silver_processing():
    wait_for_bronze = NewBronzeFileSensor(
        task_id="wait_for_new_bronze_file",
        fs_conn_id="fs_default",
        filepath=str(datalake_dir("bronze") / "*.json"),
        poke_interval=60,
        timeout=40 * 60,
        mode="reschedule",
        soft_fail=True,
    )

    @task
    def process() -> dict:
        spatial = load_config()["spatial"]
        summary = process_bronze_files(
            datalake_dir("bronze"), datalake_dir("silver"),
            radius_km=spatial["volcano_radius_km"], min_magnitude=spatial["min_magnitude"])
        log.info(json.dumps({"event": "silver_summary", **summary}, default=str))
        return {k: v for k, v in summary.items() if k != "processed"} | {"files": len(summary["processed"])}

    @task(retries=3, retry_delay=timedelta(minutes=5))
    def load_to_database() -> dict:
        summary = load_pending(datalake_dir("silver"), load_config(), mode="incremental")
        if summary["failed"]:
            raise RuntimeError(f"database load failed on {summary['failed']['file']}: {summary['failed']['error']}")
        return {"files": len(summary["loaded"]),
                **{k: sum(r.get(k, 0) for r in summary["loaded"]) for k in ("rows_in", "new", "updated", "rejected")}}

    wait_for_bronze >> process() >> load_to_database()


silver_processing()
