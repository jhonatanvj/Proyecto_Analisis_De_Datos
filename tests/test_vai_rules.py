"""Volcano Activity Indicator rules (docs/project_definition.tex, section VAI), without Spark."""
from __future__ import annotations

import math

import pytest

from volcano_pipeline.config import load_config
from volcano_pipeline.gold.vai_rules import (combine, eruptive_component, level, region_label,
                                             reported_component, seismic_component)

CFG = load_config()["indicator"]
SCORES, WEIGHTS, LEVELS, REGIONS = CFG["alert_level_scores"], CFG["weights"], CFG["levels"], CFG["regions"]


def test_reported_uses_the_alert_scores():
    assert reported_component("WATCH", SCORES) == 0.7
    assert reported_component("watch", SCORES) == 0.7
    assert reported_component(None, SCORES) is None and reported_component("UNASSIGNED", SCORES) is None


def test_zero_weeks_keep_a_quiet_volcano_at_zero():
    # 90 of 100 weeks without quakes and none now: p = (0 + 0.5 * 90) / 100 = 0.45 -> S = 0
    assert seismic_component(0, 90, 100, 50, 20) == 0.0
    # more quakes than in any past week: p = 1 -> S = 1
    assert seismic_component(100, 0, 100, 50, 20) == 1.0
    # p = (80 + 0.5 * 10) / 100 = 0.85 -> S = 0.7
    assert seismic_component(80, 10, 100, 50, 20) == pytest.approx(0.7)


def test_seismic_needs_a_baseline():
    assert seismic_component(100, 0, 100, 19, 20) is None
    assert seismic_component(0, 0, 0, 50, 20) is None


def test_eruptive_decay_and_continuing():
    assert eruptive_component(None, True, 25) == 1.0
    assert eruptive_component(0.0, False, 25) == 1.0
    assert eruptive_component(25.0, False, 25) == pytest.approx(math.exp(-1))
    assert eruptive_component(None, False, 25) is None


def test_combine_renormalises_over_available_components():
    score, used = combine({"reported": None, "seismic": 1.0, "eruptive": 0.0}, WEIGHTS)
    assert used == ["seismic", "eruptive"]
    assert score == pytest.approx(100 * 0.35 / 0.55, abs=0.05)


def test_combine_all_components():
    score, used = combine({"reported": 0.7, "seismic": 1.0, "eruptive": 1.0}, WEIGHTS)
    assert used == ["reported", "seismic", "eruptive"] and score == pytest.approx(86.5)


def test_combine_with_nothing_available():
    assert combine({"reported": None, "seismic": None, "eruptive": None}, WEIGHTS) == (None, [])


def test_levels_at_the_thresholds():
    assert level(0, LEVELS) == ("LOW", 1) and level(24.9, LEVELS) == ("LOW", 1)
    assert level(25, LEVELS) == ("MODERATE", 2) and level(50, LEVELS) == ("ELEVATED", 3)
    assert level(75, LEVELS) == ("HIGH", 4) and level(100, LEVELS) == ("HIGH", 4)


def test_region_prefers_the_observatory():
    assert region_label("avo", "Aleutian Ridge Volcanic Arc", REGIONS) == "Alaska"
    assert region_label("CVO", None, REGIONS) == "Cascades & Pacific Northwest"
    assert region_label(None, "Mariana Volcanic Arc", REGIONS) == "Northern Mariana Islands"
    assert region_label(None, "Unknown arc", REGIONS) == "Other"
