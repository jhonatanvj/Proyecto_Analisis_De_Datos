"""Rebuild the Silver files of one Bronze source after a change in its Silver rules.

Deletes the Silver topic files of the source, its seen-index (_state/seen_<topic>.parquet) and its
Bronze file names from the manifest, then processes those Bronze files again. Other sources are
not touched. Pause the silver_processing DAG first (the script refuses if a Silver run was cut
half way), e.g.:

    docker compose exec airflow-scheduler airflow dags pause silver_processing
    python scripts/reprocess_silver_source.py usgs_hans_notices
    docker compose exec airflow-scheduler airflow dags unpause silver_processing

Topics that are also loaded into the database keep their rows there; the loader only adds or
updates rows whose content changed.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

TOPIC_OF = {"usgs_hans_notices": "notices", "usgs_volcano_updates": "volcano_updates",
            "usgs_hans": "alert_notices", "gvp_volcanoes": "volcanoes", "gvp_eruptions": "eruptions"}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("source_id", choices=sorted(TOPIC_OF))
    args = ap.parse_args()

    from volcano_pipeline.bronze.envelope import bronze_files
    from volcano_pipeline.config import datalake_dir, load_config
    from volcano_pipeline.silver.run import process_bronze_files, state_dir

    bronze, silver = datalake_dir("bronze"), datalake_dir("silver")
    sdir, topic = state_dir(silver), TOPIC_OF[args.source_id]
    if (sdir / "pending.json").exists():
        print("a Silver run was interrupted (_state/pending.json): let the DAG finish one run first")
        return 1
    removed = [p for p in (silver / topic).glob(f"{topic}_*.parquet")]
    for p in removed:
        p.unlink()
    (sdir / f"seen_{topic}.parquet").unlink(missing_ok=True)
    files = bronze_files(bronze, args.source_id)
    names = {p.name for p in files}
    manifest = sdir / "manifest.json"
    done = set(json.loads(manifest.read_text(encoding="utf-8"))) - names if manifest.exists() else set()
    manifest.write_text(json.dumps(sorted(done), indent=1), encoding="utf-8")
    print(f"removed {len(removed)} {topic} files; reprocessing {len(files)} Bronze files")

    spatial = load_config()["spatial"]
    summary = process_bronze_files(bronze, silver, files=files, radius_km=spatial["volcano_radius_km"],
                                   min_magnitude=spatial["min_magnitude"])
    print(json.dumps({k: v for k, v in summary.items() if k != "processed"}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
