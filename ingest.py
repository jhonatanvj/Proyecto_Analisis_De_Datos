#!/usr/bin/env python
"""Command-line entry point (course contract).

    python ingest.py --mode historical [--start 2010-01-01 --end 2025-12-31]
    python ingest.py --mode incremental
    python ingest.py --mode status

historical:  Bronze for the historical sources (GVP volcanoes and eruptions, HANS volcano list when
             due, FDSN earthquakes and the HANS notice archive between --start and --end), then
             Silver, then the database.
incremental: Bronze for the incremental sources (HANS, USGS feed, Volcano Updates, HANS notices
             since the watermark), then Silver, then the database.
status:      sources, watermarks, last runs and row counts in the database.

Both loading modes are idempotent: Silver keeps only new or changed records, and the database
loader skips Silver files it already loaded and only rewrites rows whose content hash changed.
--skip-fetch loads what is already on disk (Silver files not in the database yet) without
calling any API, e.g. after the Airflow DAGs have been running.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
from volcano_pipeline.config import datalake_dir, load_config  # noqa: E402

log = logging.getLogger("ingest")

HISTORICAL_SOURCES = ["gvp_volcanoes", "gvp_eruptions", "usgs_hans_volcanoes"]
INCREMENTAL_SOURCES = ["usgs_hans", "usgs_feed", "usgs_volcano_updates", "usgs_hans_notices", "usgs_fdsn"]


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Volcanic Activity Monitoring - ingestion CLI")
    p.add_argument("--mode", required=True, choices=["historical", "incremental", "status"])
    p.add_argument("--start", type=date.fromisoformat, help="historical start date (YYYY-MM-DD)")
    p.add_argument("--end", type=date.fromisoformat, help="historical end date (YYYY-MM-DD, default today)")
    p.add_argument("--source", help="restrict the Bronze step to one source_id")
    p.add_argument("--skip-fetch", action="store_true",
                   help="do not call the APIs: process pending Bronze files and load pending Silver files")
    return p.parse_args(argv)


def _day(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


def fetch_bronze(mode: str, args, cfg) -> bool:
    from volcano_pipeline.bronze.run import run_bronze
    from volcano_pipeline.config import user_agent
    from volcano_pipeline.sources.http import make_session

    session = make_session(user_agent(cfg))
    sources = HISTORICAL_SOURCES if mode == "historical" else INCREMENTAL_SOURCES
    if args.source:
        sources = [s for s in sources if s == args.source]
    ok = True
    for r in run_bronze(sources, cfg, session, datalake_dir("bronze"), datetime.now(timezone.utc)):
        log.info(json.dumps({"event": "bronze", **r}))
        ok &= r["status"] in ("written", "unchanged", "not_due")

    if mode == "historical" and args.source in (None, "usgs_fdsn"):
        from volcano_pipeline.bronze.fdsn_backfill import run_fdsn_backfill, volcano_coords_from_bronze

        coords = volcano_coords_from_bronze(datalake_dir("bronze"))
        if not coords:
            log.error("no gvp_volcanoes Bronze file yet: cannot place the FDSN queries")
            return False
        start = _day(args.start or date.fromisoformat(cfg["project"]["historical_start"]))
        end = _day(args.end + timedelta(days=1)) if args.end else _day(date.today() + timedelta(days=1))
        log.info(json.dumps({"event": "fdsn_backfill", "start": start.isoformat(), "end": end.isoformat(),
                             "volcanoes": len(coords)}))
        summary = run_fdsn_backfill(coords, cfg, session, datalake_dir("bronze"), start, end)
        log.info(json.dumps({"event": "fdsn_backfill_done", **summary}, default=str))

    if mode == "historical" and args.source in (None, "usgs_hans_notices"):
        from volcano_pipeline.bronze.hans_notices import run_backfill
        from volcano_pipeline.config import get_source

        src = get_source(cfg, "usgs_hans_notices")
        start = _day(args.start or date.fromisoformat(src.get("historical_start", cfg["project"]["historical_start"])))
        end = _day(args.end + timedelta(days=1)) if args.end else datetime.now(timezone.utc)
        summary = run_backfill(cfg, session, datalake_dir("bronze"), start, end)
        log.info(json.dumps({"event": "hans_notices_backfill_done", **summary}))
    return ok


def process_silver(cfg) -> None:
    from volcano_pipeline.silver.run import process_bronze_files

    spatial = cfg["spatial"]
    summary = process_bronze_files(datalake_dir("bronze"), datalake_dir("silver"),
                                   radius_km=spatial["volcano_radius_km"], min_magnitude=spatial["min_magnitude"])
    log.info(json.dumps({"event": "silver", "bronze_files": len(summary["processed"]),
                         "rows": summary["rows"], "rejected": summary["rejected"]}))


def load_database(mode: str, cfg) -> bool:
    from volcano_pipeline.storage.db import describe
    from volcano_pipeline.storage.loader import load_pending

    log.info("loading Silver files into %s", describe())
    summary = load_pending(datalake_dir("silver"), cfg, mode=mode)
    rows = {k: sum(r.get(k, 0) for r in summary["loaded"]) for k in ("rows_in", "new", "updated", "rejected")}
    print(f"database: {len(summary['loaded'])} Silver files loaded "
          f"(rows in {rows['rows_in']}, new {rows['new']}, updated {rows['updated']}, rejected {rows['rejected']})")
    if summary["failed"]:
        print(f"database: FAILED on {summary['failed']['file']}: {summary['failed']['error']}")
        return False
    return True


def mode_status(cfg) -> int:
    print(f"Hazard: {cfg['project']['hazard']}  scope: {cfg['project']['geographic_scope']}")
    from volcano_pipeline.storage.db import connect, describe

    try:
        conn = connect()
    except Exception as exc:
        print(f"\nDatabase {describe()} not reachable ({type(exc).__name__}). "
              "Start it with: docker compose up -d project-db")
        return 1
    with conn, conn.cursor() as cur:
        cur.execute("""
SELECT s.source_id, s.role, coalesce(st.watermark, '-'), coalesce(to_char(st.last_success, 'YYYY-MM-DD HH24:MI'), '-'),
       (SELECT status FROM ingestion_run r WHERE r.source_id = s.source_id ORDER BY run_id DESC LIMIT 1)
FROM source s LEFT JOIN ingestion_state st USING (source_id) ORDER BY s.role, s.source_id""")
        print(f"\n{'source_id':<22} {'role':<12} {'watermark':<30} {'last success':<17} last run")
        for source, role, mark, last, status in cur.fetchall():
            print(f"{source:<22} {role:<12} {mark:<30} {last:<17} {status or '-'}")
        cur.execute("""
SELECT 'volcano', count(*) FROM volcano UNION ALL SELECT 'event', count(*) FROM event
UNION ALL SELECT 'eruption', count(*) FROM eruption UNION ALL SELECT 'earthquake', count(*) FROM earthquake
UNION ALL SELECT 'volcano_alert', count(*) FROM volcano_alert UNION ALL SELECT 'volcano_update', count(*) FROM volcano_update
UNION ALL SELECT 'rejected_record', count(*) FROM rejected_record UNION ALL SELECT 'ingestion_run', count(*) FROM ingestion_run
UNION ALL SELECT 'event_group', count(*) FROM event_group UNION ALL SELECT 'volcano_notice', count(*) FROM volcano_notice""")
        print("\nRows: " + ", ".join(f"{t} {n:,}" for t, n in cur.fetchall()))
        cur.execute("SELECT count(*) FROM silver_load_file")
        loaded = cur.fetchone()[0]
    from volcano_pipeline.storage.loader import silver_files

    on_disk = len(silver_files(datalake_dir("silver")))
    print(f"Silver files: {on_disk} on disk, {loaded} loaded ({max(on_disk - loaded, 0)} pending)")
    return 0


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = parse_args(argv)
    cfg = load_config()
    if args.mode == "status":
        return mode_status(cfg)
    if args.mode == "historical":
        start = args.start or date.fromisoformat(cfg["project"]["historical_start"])
        if start > (args.end or date.today()):
            log.error("--start must be <= --end")
            return 2
    ok = True
    if not args.skip_fetch:
        ok = fetch_bronze(args.mode, args, cfg)
    process_silver(cfg)
    ok &= load_database(args.mode, cfg)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
