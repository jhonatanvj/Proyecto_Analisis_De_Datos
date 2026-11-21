"""Run the Bronze step for several sources outside Airflow (same steps as the DAG:
extract -> validate -> persist). Used by scripts/run_pipeline_local.py."""
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

from .envelope import build_envelope, write_bronze
from .extract import MONTHLY_SOURCES, after_persist, extract, gvp_due
from .validate import validate_payload

log = logging.getLogger(__name__)


def run_bronze(source_ids: list[str], cfg: dict, session, bronze_dir: Path, now: datetime) -> list[dict]:
    results = []
    for source_id in source_ids:
        if source_id in MONTHLY_SOURCES and not gvp_due(bronze_dir, source_id, now):
            results.append({"source_id": source_id, "status": "not_due", "file": None})
            continue
        try:
            ex = extract(source_id, cfg, session, bronze_dir)
        except Exception as exc:   # one source down must not stop the others
            log.exception("extract failed for %s", source_id)
            results.append({"source_id": source_id, "status": "failed", "file": None, "errors": [repr(exc)]})
            continue
        errors = validate_payload(source_id, ex.payload)
        if errors:
            results.append({"source_id": source_id, "status": "invalid", "file": None, "errors": errors})
            continue
        envelope = build_envelope(source_id, ex.url, ex.params, ex.http_status, ex.content, ex.payload, now)
        path = write_bronze(envelope, bronze_dir)
        after_persist(source_id, ex.payload, bronze_dir, now)
        results.append({"source_id": source_id, "status": "written" if path else "unchanged",
                        "file": path.name if path else None})
    return results
