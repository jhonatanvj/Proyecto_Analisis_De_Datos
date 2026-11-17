"""One-off historical load: earthquakes around every US volcano from the USGS FDSN service.

Run it by hand (Trigger DAG w/ config) once. Parameters:
  * start / end: date range, default config `historical_start` (2010-01-01) -> today
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
from volcano_pipeline.bronze.fdsn_backfill import (fetch_partition, volcano_coords_from_bronze,
                                                   volcano_partitions, write_bronze_unique)
from volcano_pipeline.bronze.validate import validate_payload
from volcano_pipeline.config import datalake_dir, load_config, user_agent
from volcano_pipeline.sources.http import make_session

log = logging.getLogger("bronze_backfill_fdsn")


def _log(event: str, **fields) -> None:
    log.info(json.dumps({"event": event, **fields}, default=str))


def _day(value: str | None, default: date) -> datetime:
    d = date.fromisoformat(value) if value else default
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


@dag(
    dag_id="bronze_backfill_fdsn",
    schedule=None,                      # manual only
    start_date=pendulum.datetime(2026, 11, 16, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=1), "retry_exponential_backoff": True},
    params={
        "start": Param("2010-01-01", type="string", description="YYYY-MM-DD"),
        "end": Param("", type="string", description="YYYY-MM-DD, empty = today"),
        "vnums": Param([], type="array", description="volcano numbers to load; empty = all US volcanoes"),
    },
    tags=["volcano", "bronze", "historical"],
    doc_md=__doc__,
)
def bronze_backfill_fdsn():

    @task
    def plan(params: dict | None = None) -> list[dict]:
        cfg = load_config()
        start = _day(params.get("start"), date.fromisoformat(cfg["project"]["historical_start"]))
        end = _day(params.get("end"), datetime.now(timezone.utc).date() + timedelta(days=1))
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
