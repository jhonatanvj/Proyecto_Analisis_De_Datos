"""Copy the newest pipeline outputs into workshop_2/samples/ for the Workshop 2 delivery.

    python scripts/collect_w2_samples.py [--per-source 3]

Bronze: the newest N files of each source except gvp_eruptions (11 MB, too big for the repo).
Silver: the newest N Parquet files of each topic, plus the newest quarantine file.
"""
from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from volcano_pipeline.bronze.envelope import bronze_files  # noqa: E402

BRONZE_SOURCES = ["usgs_hans", "usgs_feed", "usgs_volcano_updates", "gvp_volcanoes"]
SILVER_TOPICS = ["earthquakes", "alert_notices", "volcanoes", "eruptions", "volcano_updates"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--per-source", type=int, default=3)
    n = ap.parse_args().per_source
    out = ROOT / "workshop_2" / "samples"
    for sub in ("bronze", "silver", "quarantine"):
        (out / sub).mkdir(parents=True, exist_ok=True)

    bronze, silver = ROOT / "datalake_bronze", ROOT / "datalake_silver"
    copied = []
    for source in BRONZE_SOURCES:
        copied += [shutil.copy2(p, out / "bronze") for p in bronze_files(bronze, source)[-n:]]
    for topic in SILVER_TOPICS:
        copied += [shutil.copy2(p, out / "silver") for p in sorted(silver.glob(f"{topic}_*.parquet"))[-n:]]
    copied += [shutil.copy2(p, out / "quarantine") for p in sorted((silver / "quarantine").glob("rejected_*.parquet"))[-1:]]
    for p in copied:
        print(Path(p).relative_to(ROOT))


if __name__ == "__main__":
    main()
