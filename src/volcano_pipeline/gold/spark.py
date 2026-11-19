"""SparkSession for the Gold layer: local mode inside the Airflow container.

Settings come from the environment (docker-compose.yml), so they can be tuned without code:

* SPARK_MASTER (default local[*]): every core of the container, no cluster.
* SPARK_DRIVER_MEMORY (default 1g): in local mode the driver also runs the executors, so this
  is all the memory Spark gets. Docker Desktop needs about 6 GB for Airflow plus this.
* SPARK_SHUFFLE_PARTITIONS (default 8): Spark's default of 200 is meant for clusters; our largest
  Silver topic (~300k earthquakes) fits easily in 8 partitions.

The Spark UI is disabled (no port is published for it) and the session time zone is UTC, like
every timestamp in Silver. GVP eruptions go back to the Holocene, so Parquet dates before 1582
are read and written as proleptic Gregorian (CORRECTED) instead of failing.
"""
from __future__ import annotations

import os


def get_spark(app_name: str = "volcano-gold"):
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .appName(app_name)
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "1g"))
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
    )
    return builder.getOrCreate()
