"""Gold processing: read every Silver Parquet file with PySpark and write the two Gold summaries.

Runs weekly (Monday 03:00 UTC), after a full week of hourly Bronze/Silver runs; it can also be
triggered by hand from the UI. Each task starts its own local SparkSession (local[*], driver
memory from SPARK_DRIVER_MEMORY), reads datalake_silver/<topic>/ folders with
spark.read.parquet and writes one file:

* build_governance   -> datalake_gold/governance_YYYYMMDD_HHMMSS.parquet   (data-quality KPIs)
* build_storytelling -> datalake_gold/storytelling_YYYYMMDD_HHMMSS.parquet (dashboard aggregations)

The tasks run one after the other, not in parallel, so two JVMs never compete for the memory of
the container. Both files use the same long format (volcano_pipeline/gold/contract.py) and are
the only data source of the Workshop 4 dashboards. check_silver skips the run when Silver is
still empty (fresh clone before the first Silver run).
"""
from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException

from volcano_pipeline.config import datalake_dir
from volcano_pipeline.gold.silver_reader import TOPICS, topic_dir

log = logging.getLogger("gold_processing")


@dag(
    dag_id="gold_processing",
    schedule="0 3 * * 1",
    start_date=pendulum.datetime(2026, 11, 16, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5),
                  "execution_timeout": timedelta(minutes=30)},
    tags=["volcano", "gold", "pyspark"],
    doc_md=__doc__,
)
def gold_processing():
    @task
    def check_silver() -> list[str]:
        present = [t for t in TOPICS if any(topic_dir(datalake_dir("silver"), t).glob("*.parquet"))]
        log.info("Silver topics with data: %s", present)
        if not present:
            raise AirflowSkipException("datalake_silver/ has no topic files yet")
        return present

    @task
    def build_governance(topics: list[str]) -> dict:
        from volcano_pipeline.gold.run import run_summary

        return run_summary("governance", datalake_dir("silver"), datalake_dir("gold"))

    @task
    def build_storytelling() -> dict:
        from volcano_pipeline.gold.run import run_summary

        return run_summary("storytelling", datalake_dir("silver"), datalake_dir("gold"))

    build_governance(check_silver()) >> build_storytelling()


gold_processing()
