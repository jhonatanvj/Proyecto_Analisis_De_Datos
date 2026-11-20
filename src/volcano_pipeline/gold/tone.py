"""Activity tone scoring for volcanic notices: domain lexicon vs. VADER.

Standard sentiment analyzers (like VADER, tuned for social media) misclassify technical
volcanology language—e.g., treating "normal" or "quiet" as neutral/slightly positive while
missing domain escalation cues such as "inflation", "tremor", or "ash plume".

This module provides:
1. A weighted volcanic-domain lexicon scorer in [-1.0, +1.0], where positive values
   indicate escalating unrest/eruption and negative values indicate de-escalation/calm.
2. A VADER compound scorer in [-1.0, +1.0] for direct side-by-side comparison.
"""
from __future__ import annotations

import math
import re

ESCALATION_WEIGHTS: dict[str, float] = {
    "eruption": 2.0,
    "erupting": 2.0,
    "eruptive": 1.8,
    "explosion": 2.0,
    "explosive": 1.8,
    "pyroclastic": 2.0,
    "lahar": 2.0,
    "lava": 1.7,
    "ash": 1.6,
    "plume": 1.5,
    "tremor": 1.5,
    "swarm": 1.5,
    "inflation": 1.4,
    "deformation": 1.4,
    "unrest": 1.4,
    "elevated": 1.3,
    "intensified": 1.4,
    "increased": 1.2,
    "increasing": 1.2,
    "incandescence": 1.5,
    "fountaining": 1.7,
    "ballistics": 1.6,
    "so2": 1.2,
    "sulfur": 1.1,
    "seismic": 1.1,
    "seismicity": 1.1,
    "steam": 0.8,
    "warning": 1.8,
    "watch": 1.4,
    "orange": 1.4,
    "red": 1.8,
}

DEESCALATION_WEIGHTS: dict[str, float] = {
    "decrease": -1.2,
    "decreased": -1.3,
    "decreasing": -1.2,
    "decline": -1.2,
    "declined": -1.3,
    "declining": -1.2,
    "subsided": -1.5,
    "subsiding": -1.4,
    "paused": -1.4,
    "ceased": -1.6,
    "ended": -1.5,
    "diminished": -1.3,
    "quiescent": -1.6,
    "quiet": -1.4,
    "normal": -1.5,
    "background": -1.3,
    "low": -0.9,
    "green": -1.5,
    "stable": -1.0,
}

_TOKEN_RE = re.compile(r"[a-z0-9_]+")
_VADER_ANALYZER = None


def _get_vader():
    global _VADER_ANALYZER
    if _VADER_ANALYZER is None:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

        _VADER_ANALYZER = SentimentIntensityAnalyzer()
    return _VADER_ANALYZER


def lexicon_score(text: str | None) -> float:
    """Compute normalized volcanic activity tone in [-1.0, +1.0].

    Positive values signal escalating activity; negative values signal de-escalation or
    background/normal conditions. Uses VADER-style asymptotic normalization so longer
    summaries do not saturate artificially.
    """
    if not text or not text.strip():
        return 0.0
    tokens = _TOKEN_RE.findall(text.lower())
    if not tokens:
        return 0.0

    raw_sum = 0.0
    for i, tok in enumerate(tokens):
        prev = tokens[i - 1] if i > 0 else ""
        if tok in ESCALATION_WEIGHTS:
            w = ESCALATION_WEIGHTS[tok]
            raw_sum += -0.6 * w if prev in {"no", "not", "without"} else w
        elif tok in DEESCALATION_WEIGHTS:
            raw_sum += DEESCALATION_WEIGHTS[tok]

    if raw_sum == 0.0:
        return 0.0
    return round(raw_sum / math.sqrt(raw_sum * raw_sum + 4.0), 4)


def vader_score(text: str | None) -> float:
    """Compute VADER compound sentiment score in [-1.0, +1.0]."""
    if not text or not text.strip():
        return 0.0
    return round(float(_get_vader().polarity_scores(text)["compound"]), 4)
