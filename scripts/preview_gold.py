"""Print a readable preview of the newest Gold summary (evidence for the Workshop 3 report).

    python scripts/preview_gold.py                 # newest governance_*.parquet
    python scripts/preview_gold.py storytelling    # newest storytelling_*.parquet

Shows the file name and size, the rows and metrics per section, and a few representative rows.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
COLUMNS = ["section", "metric", "dimension", "period", "value", "unit"]
# (section, pandas query to pick telling rows, max rows) for the governance summary
GOVERNANCE_SAMPLE = [
    ("null_rate", "value > 10", 3),
    ("duplicate_rate", "dimension in ['earthquakes', 'usgs_fdsn']", 6),
    ("schema_compliance", "metric == 'pct_files_conforming'", 2),
    ("quarantine_rate", "dimension == 'all sources' or metric == 'records_by_reason'", 3),
    ("outlier_rate", "metric == 'pct_outlier'", 2),
    ("text_length", "dimension == 'notices'", 3),
    ("freshness", "value == value", 2),
    ("ingestion_frequency", "metric == 'pct_of_expected'", 2),
    ("volume_total", "value == value", 2),
]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("kind", nargs="?", default="governance", choices=["governance", "storytelling"])
    parser.add_argument("--gold", type=Path, default=ROOT / "datalake_gold")
    args = parser.parse_args()

    files = sorted(args.gold.glob(f"{args.kind}_*.parquet"))
    if not files:
        raise SystemExit(f"no {args.kind}_*.parquet in {args.gold}: run the gold_processing DAG first")
    path = files[-1]
    df = pd.read_parquet(path)
    pd.set_option("display.width", 170)
    pd.set_option("display.max_colwidth", 60)

    print(f"{path.name}  ({path.stat().st_size / 1024:.0f} KB, {len(df):,} rows x {df.shape[1]} columns, "
          f"generated {df.generated_utc.max():%Y-%m-%d %H:%M} UTC)\n")
    per_section = df.groupby("section").agg(rows=("value", "size"),
                                            metrics=("metric", lambda s: ", ".join(sorted(s.unique()))))
    print(per_section.to_string(), "\n")

    if args.kind == "governance":
        sample = pd.concat([df[df.section == s].query(q).head(n) for s, q, n in GOVERNANCE_SAMPLE])
    else:
        sample = df.groupby("section").head(2)
    print(sample[COLUMNS].round(3).to_string(index=False))


if __name__ == "__main__":
    main()
