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


def _rows(section, metric, dimension, values, unit, period=None):
    return [{"section": section, "metric": m, "dimension": dimension, "period": period, "value": v, "unit": unit}
            for m, v in zip(metric, values)]


# Volcano Activity Indicator sections (Workshop 4 Part 1): an Alaska volcano on WATCH with a weekly
# history and a Hawaii volcano with a continuing eruption and no seismic baseline
_AK, _HI = "311120|Great Sitkin|Alaska|avo", "332010|Kilauea|Hawaii & American Samoa|hvo"
INDICATOR_ROWS = (
    _rows("volcano_map", ["latitude", "longitude", "alert_level_ord", "color_code_ord"], _AK,
          [52.08, -176.13, 3, 3], "deg")
    + _rows("volcano_map", ["latitude", "longitude", "alert_level_ord", "color_code_ord"], _HI,
            [19.42, -155.29, 1, 1], "deg")
    + _rows("volcano_map", ["latitude", "longitude", "alert_level_ord", "color_code_ord"],
            "244001|Ta'u|Hawaii & American Samoa|hvo", [-14.23, -169.45, None, None], "deg")
    + _rows("vai_current", ["vai_score", "level_ord", "components_used", "reported", "seismic", "eruptive",
                            "earthquakes_window", "baseline_percentile"], "311120",
            [72.2, 3, 2, 0.7, 0.75, None, 10, 0.875], "score")
    + _rows("vai_current", ["vai_score", "level_ord", "components_used", "reported", "seismic", "eruptive",
                            "earthquakes_window", "baseline_percentile"], "332010",
            [100.0, 4, 2, 0.0, None, 1.0, 0, None], "score")
    + [r for w, s in [(date(2026, 11, 2), 20.0), (date(2026, 11, 9), 45.0), (date(2026, 11, 16), 60.0)]
       for r in _rows("vai_weekly", ["vai_score"], "311120", [s], "score", w)]
    # one eruption inside the weekly history and one before it starts (not drawn)
    + _rows("eruption_history", ["vei", "continuing"], "311120", [2.0, 1.0], "VEI", date(2026, 11, 10))
    + _rows("eruption_history", ["vei", "continuing"], "311120", [None, 0.0], "VEI", date(2019, 6, 1))
)
