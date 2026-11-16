"""Run the Bronze and Silver steps once, without Docker or Airflow.

Same code as the DAGs, handy to test a change quickly or to debug a source:

    python scripts/run_pipeline_local.py                    # Bronze for all sources, then Silver
    python scripts/run_pipeline_local.py --only bronze --sources usgs_hans usgs_volcano_updates
    python scripts/run_pipeline_local.py --only silver
    python scripts/run_pipeline_local.py --root /tmp/lake   # write to another folder, not the repo

Outputs go to datalake_bronze/ and datalake_silver/ under --root (default: the repo, same
folders the Docker stack uses, so do not run this while the DAGs are running).
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

ALL_SOURCES = ["gvp_volcanoes", "gvp_eruptions", "usgs_hans", "usgs_feed", "usgs_volcano_updates"]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--only", choices=["bronze", "silver"], help="run just one layer")
    ap.add_argument("--sources", nargs="+", choices=ALL_SOURCES, default=ALL_SOURCES)
    ap.add_argument("--root", help="folder that holds datalake_bronze/ and datalake_silver/")
    args = ap.parse_args()
    if args.root:
        os.environ["DATALAKE_ROOT"] = str(Path(args.root).resolve())

    # imported after DATALAKE_ROOT is set
    from volcano_pipeline.bronze.run import run_bronze
    from volcano_pipeline.config import datalake_dir, load_config, user_agent
    from volcano_pipeline.silver.run import process_bronze_files
    from volcano_pipeline.sources.http import make_session

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    cfg = load_config()
    ok = True
    if args.only in (None, "bronze"):
        results = run_bronze(args.sources, cfg, make_session(user_agent(cfg)),
                             datalake_dir("bronze"), datetime.now(timezone.utc))
        for r in results:
            print(json.dumps(r))
        ok = all(r["status"] in ("written", "unchanged", "not_due") for r in results)
    if args.only in (None, "silver"):
        spatial = cfg["spatial"]
        summary = process_bronze_files(datalake_dir("bronze"), datalake_dir("silver"),
                                       radius_km=spatial["volcano_radius_km"],
                                       min_magnitude=spatial["min_magnitude"])
        print(json.dumps({k: v for k, v in summary.items() if k != "processed"}
                         | {"bronze_files": len(summary["processed"])}))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
