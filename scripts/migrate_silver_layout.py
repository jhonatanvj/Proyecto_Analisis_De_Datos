"""Move Silver topic files from datalake_silver/ into datalake_silver/<topic>/ (Workshop 3 layout).

Workshop 2 wrote every topic into the same folder. PySpark reads a whole folder as one
DataFrame, so each topic now has its own folder. Run once per clone, with the DAGs paused:

    python scripts/migrate_silver_layout.py [--root /path/holding/datalake_silver]

Safe to run twice. File names do not change, so the database loader does not load them again.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TOPIC_FILE = re.compile(r"^(?P<topic>volcanoes|eruptions|earthquakes|alert_notices|volcano_updates)_\d{8}_\d{6}\.parquet$")


def migrate(silver_dir: Path) -> list[str]:
    if (silver_dir / "_state" / "pending.json").exists():
        raise SystemExit("a Silver run was interrupted (_state/pending.json exists): run the Silver DAG once first")
    moved = []
    for path in sorted(silver_dir.glob("*.parquet")):
        m = TOPIC_FILE.match(path.name)
        if not m:
            continue
        target = silver_dir / m.group("topic") / path.name
        target.parent.mkdir(exist_ok=True)
        path.replace(target)
        moved.append(target.relative_to(silver_dir).as_posix())
    return moved


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", default=str(ROOT), help="folder that holds datalake_silver/")
    silver_dir = Path(ap.parse_args().root) / "datalake_silver"
    if not silver_dir.exists():
        print(f"nothing to do: {silver_dir} does not exist")
        return 0
    moved = migrate(silver_dir)
    for name in moved:
        print(f"moved {name}")
    print(f"{len(moved)} files moved")
    return 0


if __name__ == "__main__":
    sys.exit(main())
