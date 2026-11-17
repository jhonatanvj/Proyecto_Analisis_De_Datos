"""Silver processing: clean the new Bronze files and write Parquet files to datalake_silver/.

A FileSensor watches datalake_bronze/*.json. The plain FileSensor would succeed as soon as any
Bronze file exists, including files already processed, so NewBronzeFileSensor also checks the
Silver manifest and only succeeds when at least one file is new.

Timing: Bronze runs at minute 0 of every hour; this DAG starts at minute 15 and pokes every
60 s for up to 40 min (mode=reschedule frees the worker slot between pokes). If no new file
shows up the run ends as skipped (soft_fail), not failed, because "nothing new this hour" is a
normal outcome when the sources did not change.
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

    wait_for_bronze >> process()


silver_processing()
