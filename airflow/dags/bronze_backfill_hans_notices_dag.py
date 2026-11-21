"""One-off historical load of the USGS HANS notice archive (full text of every volcano notice).

Run it by hand (Trigger DAG w/ config) once. Parameters:
  * start / end: date range YYYY-MM-DD; leave start blank for historical_start of
    usgs_hans_notices in sources.yaml (2010-01-01) and end blank for now

The archive is read one calendar month per Bronze file (usgs_hans_notices_YYYYMMDD_HHMMSS.json),
oldest first, one search page per second. Months already loaded are listed in
datalake_bronze/_state/usgs_hans_notices.backfill_done and skipped, so a run that stopped half
way is simply triggered again. Full archive since 2010: about 16,500 notices, ~830 pages,
~140 MB of Bronze JSON and 20-25 minutes. The hourly bronze_ingestion DAG then keeps the archive
current from the watermark this backfill leaves behind; silver_processing turns the files into
the Silver topic notices.
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param

from volcano_pipeline.bronze.fdsn_backfill import parse_day
from volcano_pipeline.bronze.hans_notices import SOURCE_ID, run_backfill
from volcano_pipeline.config import datalake_dir, get_source, load_config, user_agent
from volcano_pipeline.sources.http import make_session

log = logging.getLogger("bronze_backfill_hans_notices")


@dag(
    dag_id="bronze_backfill_hans_notices",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 19, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2},
    params={
        "start": Param("", type=["null", "string"], description="YYYY-MM-DD, blank = historical_start"),
        "end": Param("", type=["null", "string"], description="YYYY-MM-DD, blank = now"),
    },
    tags=["volcano", "bronze", "backfill"],
    doc_md=__doc__,
)
def bronze_backfill_hans_notices():
    @task
    def backfill(params=None) -> dict:
        cfg = load_config()
        default_start = date.fromisoformat(get_source(cfg, SOURCE_ID).get("historical_start", "2010-01-01"))
        start = parse_day(params.get("start"), default_start)
        end_text = (params.get("end") or "").strip()
        end = parse_day(end_text, date.today()) if end_text else datetime.now(timezone.utc)
        summary = run_backfill(cfg, make_session(user_agent(cfg)), datalake_dir("bronze"), start, end)
        log.info(json.dumps({"event": "hans_notices_backfill", "start": start.isoformat(),
                             "end": end.isoformat(), **summary}))
        return summary

    backfill()


bronze_backfill_hans_notices()
