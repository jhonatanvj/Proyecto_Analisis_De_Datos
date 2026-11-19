"""One-off historical load: earthquakes around every US volcano from the USGS FDSN service.

Run it by hand (Trigger DAG w/ config) once. Parameters:
  * start / end: date range YYYY-MM-DD; leave end blank for today
  * vnums: optional list of volcano numbers to load only those (e.g. [332010] for Kilauea)

plan asks the FDSN /count endpoint how many events each volcano has and splits busy volcanoes
into date ranges under the 20,000-events-per-request limit. fetch downloads each range as
GeoJSON (at most 4 requests at the same time, to be polite with USGS) and writes it to
datalake_bronze/ as usgs_fdsn_YYYYMMDD_HHMMSS.json. The silver_processing DAG then picks the
files up like any other Bronze file; the event ids are the same as in the real-time feed, so
events present in both are not duplicated.

Full run (165 volcanoes, 2010 -> today): about 365,000 events, a few hundred requests and
roughly 350 MB of Bronze JSON. Re-running it does not duplicate Silver rows (unchanged
events are skipped), but it does download everything again.
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta, timezone

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param

from volcano_pipeline.bronze.envelope import build_envelope
from volcano_pipeline.bronze.fdsn_backfill import (fetch_partition, parse_day, volcano_coords_from_bronze,
                                                   volcano_partitions, write_bronze_unique)
from volcano_pipeline.bronze.fdsn_incremental import advance_watermark
from volcano_pipeline.bronze.validate import validate_payload
from volcano_pipeline.config import datalake_dir, load_config, user_agent
from volcano_pipeline.sources.http import make_session

log = logging.getLogger("bronze_backfill_fdsn")


def _log(event: str, **fields) -> None:
    log.info(json.dumps({"event": event, **fields}, default=str))


@dag(
    dag_id="bronze_backfill_fdsn",
    schedule=None,                      # manual only
    start_date=pendulum.datetime(2026, 11, 16, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=1), "retry_exponential_backoff": True},
    params={
        # the trigger form sends an emptied field as null, so every field also accepts null
        "start": Param("2010-01-01", type=["null", "string"], description="YYYY-MM-DD"),
        "end": Param(None, type=["null", "string"], description="YYYY-MM-DD, leave blank for today"),
        "vnums": Param(None, type=["null", "array"],
                       description="GVP volcano numbers, one per line (e.g. 332010 = Kilauea); blank = all US volcanoes"),
    },
    tags=["volcano", "bronze", "historical"],
    doc_md=__doc__,
)
def bronze_backfill_fdsn():

    @task
    def plan(params: dict | None = None) -> list[dict]:
        cfg = load_config()
        try:
            start = parse_day(params.get("start"), date.fromisoformat(cfg["project"]["historical_start"]))
            end = parse_day(params.get("end"), datetime.now(timezone.utc).date() + timedelta(days=1))
        except ValueError as exc:   # a typo in the form will not fix itself: no retries
            raise AirflowFailException(str(exc)) from None
        if start >= end:
            raise AirflowFailException(f"start {start:%Y-%m-%d} must be before end {end:%Y-%m-%d}")
        vnums = [int(v) for v in params.get("vnums") or []]
        coords = volcano_coords_from_bronze(datalake_dir("bronze"), only=vnums)
        if not coords:
            raise AirflowFailException("no volcano positions: run bronze_ingestion first (it downloads gvp_volcanoes)")
        parts = volcano_partitions(coords, cfg, make_session(user_agent(cfg)), start, end)
        _log("planned", volcanoes=len(coords), partitions=len(parts),
             expected_events=sum(p["expected"] for p in parts), start=start, end=end)
        return parts

    @task(max_active_tis_per_dag=4)
    def fetch(part: dict) -> dict:
        cfg = load_config()
        ex = fetch_partition(part, cfg, make_session(user_agent(cfg)))
        errors = validate_payload(ex.source_id, ex.payload)
        if errors:
            raise AirflowFailException(f"vnum {part['vnum']}: {errors}")
        envelope = build_envelope(ex.source_id, ex.url, ex.params, ex.http_status, ex.content,
                                  ex.payload, datetime.now(timezone.utc))
        path = write_bronze_unique(envelope, datalake_dir("bronze"))
        advance_watermark(datalake_dir("bronze"), envelope["payload"])   # start point of the incremental mode
        result = {"vnum": part["vnum"], "start": part["start"], "end": part["end"],
                  "events": envelope["record_count"], "expected": part["expected"],
                  "file": path.name if path else None}
        _log("fetched", **result)
        return result

    @task(trigger_rule="all_done")
    def summary(results: list[dict]) -> dict:
        results = [r for r in results if r]
        total = {"partitions": len(results), "events": sum(r["events"] for r in results),
                 "files": sum(1 for r in results if r["file"])}
        _log("backfill_summary", **total)
        return total

    summary(fetch.expand(part=plan()))


bronze_backfill_fdsn()
