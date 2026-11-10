#!/usr/bin/env python
"""Command-line entry point (course contract).

    python ingest.py --mode historical [--start 2010-01-01 --end 2025-12-31]
    python ingest.py --mode incremental
    python ingest.py --mode status

Workshop #1: stub. Modes validate arguments and configuration and report what
they *will* do; the real implementations arrive in Workshops #2 (historical)
and #3 (incremental). ``status`` already lists sources and raw-zone contents.
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

sys.path.insert(0, "src")
from volcano_pipeline.config import load_config, raw_dir  # noqa: E402

log = logging.getLogger("ingest")


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Volcanic Activity Monitoring - ingestion CLI")
    p.add_argument("--mode", required=True, choices=["historical", "incremental", "status"])
    p.add_argument("--start", type=date.fromisoformat, help="historical start date (YYYY-MM-DD)")
    p.add_argument("--end", type=date.fromisoformat, help="historical end date (YYYY-MM-DD)")
    p.add_argument("--source", help="restrict to one source_id")
    return p.parse_args(argv)


def mode_status(cfg) -> None:
    print(f"Hazard: {cfg['project']['hazard']}  scope: {cfg['project']['geographic_scope']}")
    print(f"{'source_id':<16} {'role':<12} {'format':<9} {'watermark':<40} raw files")
    rd = raw_dir()
    for s in cfg["sources"]:
        n = len([f for f in rd.glob(f"{s['source_id']}_*") if not f.name.endswith(".meta.json")]) if rd.exists() else 0
        print(f"{s['source_id']:<16} {s['role']:<12} {s['format']:<9} {s['watermark_field']:<40} {n}")
    print("\nDatabase watermarks / ingestion runs: not available yet (Workshop #2-#3).")


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = parse_args(argv)
    cfg = load_config()
    if args.mode == "status":
        mode_status(cfg)
    elif args.mode == "historical":
        start = args.start or date.fromisoformat(cfg["project"]["historical_start"])
        end = args.end or date.today()
        if start > end:
            log.error("--start must be <= --end")
            return 2
        log.info("historical ingestion %s -> %s: not implemented yet (Workshop #2)", start, end)
    else:
        log.info("incremental ingestion (watermark -> now): not implemented yet (Workshop #3)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
