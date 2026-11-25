"""Rules of the Volcano Activity Indicator (VAI), as plain functions.

VAI = 100 * (w_R R + w_S S + w_H H) / (sum of the weights of the available components)

R  reported activity: score of the latest USGS HANS alert level
S  seismic anomaly: earthquakes of the last days against the volcano's own weekly counts
H  eruptive state: 1 during an eruption, then exp(-years / decay) since it ended

A component that cannot be computed is None and is left out; the remaining weights are
renormalised. All numbers (weights, scores, thresholds, decay) come from config/sources.yaml,
block indicator. gold/indicator.py does the aggregation in PySpark and calls these functions on the
small per-volcano and per-week tables. The VAI describes how active a volcano looks against its own
past; it is not an eruption forecast.
"""
from __future__ import annotations

import math

COMPONENTS = ("reported", "seismic", "eruptive")


def reported_component(alert_level: str | None, scores: dict) -> float | None:
    """Score of the official alert level; None when the volcano has no (known) HANS alert."""
    if not alert_level:
        return None
    return scores.get(alert_level.strip().upper())


def seismic_component(n_below: int, n_equal: int, n_weeks: int,
                      baseline_events: int, min_events: int) -> float | None:
    """S from the mid-rank percentile of the current count among the volcano's weekly counts.

    n_below / n_equal: weeks with fewer / the same number of earthquakes as now, zero weeks included.
    The mid-rank keeps a quiet volcano at 0: with no earthquake now and 90 % zero weeks, p = 0.45.
    """
    if n_weeks <= 0 or baseline_events < min_events:
        return None
    p = (n_below + 0.5 * n_equal) / n_weeks
    return max(0.0, (p - 0.5) / 0.5)


def eruptive_component(years_since: float | None, in_eruption: bool, decay_years: float) -> float | None:
    """1 during an eruption (or a continuing one), else exp(-years since it ended / decay)."""
    if in_eruption:
        return 1.0
    if years_since is None:
        return None
    return math.exp(-max(0.0, years_since) / decay_years)


def combine(components: dict[str, float | None], weights: dict) -> tuple[float | None, list[str]]:
    """VAI score 0-100 (one decimal) and the components it used, in the order of COMPONENTS."""
    used = [c for c in COMPONENTS if components.get(c) is not None]
    total = sum(weights[c] for c in used)
    if not used or total <= 0:
        return None, []
    score = 100 * sum(weights[c] * components[c] for c in used) / total
    return round(score, 1), used


def level(score: float, levels: dict) -> tuple[str, int]:
    """Level name and ordinal (1 = lowest) of a score; levels maps name -> lower bound."""
    ordered = sorted(levels.items(), key=lambda kv: kv[1])
    name, ordinal = ordered[0][0], 1
    for i, (candidate, bound) in enumerate(ordered, start=1):
        if score >= bound:
            name, ordinal = candidate, i
    return name, ordinal


def region_label(observatory: str | None, subregion: str | None, regions: dict) -> str:
    """Region shown in the ranking filter: observatory group first, GVP subregion as fallback."""
    if observatory and observatory.strip().lower() in regions["observatories"]:
        return regions["observatories"][observatory.strip().lower()]
    return regions["subregions"].get(subregion or "", "Other")
