"""Small Gold files for the dashboard tests, written with pandas in the Gold contract shape."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

GOVERNANCE_ROWS = [
    {"section": "volume_total", "metric": "records", "dimension": "volcanoes", "value": 165.0, "unit": "records"},
    {"section": "volume_total", "metric": "records", "dimension": "earthquakes", "value": 292239.0, "unit": "records"},
    {"section": "null_rate", "metric": "pct_null", "dimension": "eruptions.vei", "value": 37.0, "unit": "%"},
]

STORYTELLING_ROWS = [
    # dimension ALL = every US volcano; a vnum = one volcano with an elevated alert
    {"section": "tone_weekly", "metric": "lexicon_mean", "dimension": "ALL", "period": date(2026, 11, 9),
     "value": 0.2, "unit": "score"},
    {"section": "tone_weekly", "metric": "lexicon_mean", "dimension": "ALL", "period": date(2026, 11, 16),
     "value": -0.1, "unit": "score"},
    {"section": "tone_weekly", "metric": "lexicon_mean", "dimension": "332010", "period": date(2026, 11, 16),
     "value": 0.3, "unit": "score"},
]


def write_gold(folder: Path, kind: str, stamp: str, rows: list[dict]) -> Path:
    """Write datalake_gold-style <kind>_<stamp>.parquet; generated_utc comes from the stamp."""
    folder.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(rows, columns=["section", "metric", "dimension", "period", "value", "unit"])
    # same dtype as the Spark-written files: naive timestamps that mean UTC
    frame["generated_utc"] = pd.Series(pd.to_datetime(stamp, format="%Y%m%d_%H%M%S"),
                                       index=frame.index, dtype="datetime64[us]")
    path = folder / f"{kind}_{stamp}.parquet"
    frame.to_parquet(path, index=False)
    return path
