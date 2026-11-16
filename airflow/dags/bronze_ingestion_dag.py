"""Bronze ingestion: pull each source and keep its payload as a JSON file in datalake_bronze/.

Sources (config/sources.yaml):
  * API: USGS HANS monitored volcanoes, USGS earthquake feed (all_week)
  * API, monthly: Smithsonian GVP volcanoes and eruptions (static catalogue)
  * Web scraping: USGS Volcano Updates page

Each source goes extract -> validate -> persist. The extract task writes a temporary envelope to
datalake_bronze/_tmp/ (the GVP eruptions layer is ~12 MB, too big for XCom) and only its path
travels between tasks. persist writes <source_id>_YYYYMMDD_HHMMSS.json, or nothing when the
content did not change since the last file of that source.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pendulum
from airflow.decorators import dag, task

from volcano_pipeline.bronze.envelope import build_envelope, write_bronze
from volcano_pipeline.bronze.extract import API_SOURCES, GVP_SOURCES, SCRAPE_SOURCE, extract, gvp_due
from volcano_pipeline.bronze.validate import validate_payload
from volcano_pipeline.config import datalake_dir, load_config, user_agent
from volcano_pipeline.sources.http import make_session

log = logging.getLogger("bronze_ingestion")


def _log(event: str, **fields) -> None:
    log.info(json.dumps({"event": event, **fields}, default=str))


def _extract_to_tmp(source_id: str) -> dict:
    cfg = load_config()
    when = datetime.now(timezone.utc)
    ex = extract(source_id, cfg, make_session(user_agent(cfg)))
    envelope = build_envelope(source_id, ex.url, ex.params, ex.http_status, ex.content, ex.payload, when)
    tmp_dir = datalake_dir("bronze") / "_tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp = tmp_dir / f"{source_id}_{when:%Y%m%d_%H%M%S}.json"
    tmp.write_text(json.dumps(envelope, ensure_ascii=False), encoding="utf-8")
    _log("extracted", source_id=source_id, records=envelope["record_count"],
         bytes=len(ex.content), sha256=envelope["content_sha256"])
    return {"source_id": source_id, "tmp_path": str(tmp)}


@dag(
    dag_id="bronze_ingestion",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 11, 16, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2), "retry_exponential_backoff": True},
    tags=["volcano", "bronze"],
    doc_md=__doc__,
)
def bronze_ingestion():

    @task
    def extract_api(source_id: str) -> dict:
        return _extract_to_tmp(source_id)

    @task
    def extract_gvp(source_id: str) -> dict:
        if not gvp_due(datalake_dir("bronze"), source_id, datetime.now(timezone.utc)):
            _log("not_due", source_id=source_id, reason="already downloaded this month")
            return {"source_id": source_id, "tmp_path": None}
        return _extract_to_tmp(source_id)

    @task
    def scrape_volcano_updates() -> dict:
        return _extract_to_tmp(SCRAPE_SOURCE)

    @task
    def validate(ref: dict) -> dict:
        if not ref["tmp_path"]:
            return ref
        tmp = Path(ref["tmp_path"])
        envelope = json.loads(tmp.read_text(encoding="utf-8"))
        errors = validate_payload(ref["source_id"], envelope["payload"])
        if errors:
            tmp.unlink(missing_ok=True)
            _log("validation_failed", source_id=ref["source_id"], errors=errors)
            raise ValueError(f"{ref['source_id']}: {errors}")
        _log("validated", source_id=ref["source_id"], records=envelope["record_count"])
        return ref

    @task
    def persist(ref: dict) -> dict:
        if not ref["tmp_path"]:
            return {"source_id": ref["source_id"], "status": "not_due", "file": None}
        tmp = Path(ref["tmp_path"])
        envelope = json.loads(tmp.read_text(encoding="utf-8"))
        path = write_bronze(envelope, datalake_dir("bronze"))
        tmp.unlink(missing_ok=True)
        status = "written" if path else "unchanged"
        _log(status, source_id=ref["source_id"], file=path.name if path else None)
        return {"source_id": ref["source_id"], "status": status, "file": path.name if path else None}

    api = extract_api.expand(source_id=list(API_SOURCES))
    persist.override(task_id="persist_api").expand(
        ref=validate.override(task_id="validate_api").expand(ref=api))

    gvp = extract_gvp.expand(source_id=list(GVP_SOURCES))
    persist.override(task_id="persist_gvp").expand(
        ref=validate.override(task_id="validate_gvp").expand(ref=gvp))

    scraped = scrape_volcano_updates()
    persist.override(task_id="persist_scrape")(validate.override(task_id="validate_scrape")(scraped))


bronze_ingestion()
