"""Unit tests for the Storytelling Dashboard (Workshop 4 Part 3)."""
from __future__ import annotations

from datetime import date

import pytest

pytest.importorskip("dash")

from dashboard import storytelling_app  # noqa: E402
from dashboard.common.gold import load_gold  # noqa: E402
from dashboard_helpers import STORYTELLING_ROWS, write_gold  # noqa: E402

FULL_STORYTELLING_ROWS = STORYTELLING_ROWS + [
    {"section": "tone_weekly", "metric": "vader_mean", "dimension": "ALL", "period": date(2026, 11, 9), "value": 0.05, "unit": "score"},
    {"section": "tone_weekly", "metric": "vader_mean", "dimension": "ALL", "period": date(2026, 11, 16), "value": -0.02, "unit": "score"},
    {"section": "tone_distribution", "metric": "records", "dimension": "escalating", "period": None, "value": 78.0, "unit": "records"},
    {"section": "tone_distribution", "metric": "records", "dimension": "stable", "period": None, "value": 14.0, "unit": "records"},
    {"section": "tone_distribution", "metric": "records", "dimension": "calming", "period": None, "value": 8.0, "unit": "records"},
    {"section": "tone_distribution", "metric": "pct_records", "dimension": "escalating", "period": None, "value": 78.0, "unit": "%"},
    {"section": "top_keywords", "metric": "frequency", "dimension": "unigram:lava", "period": None, "value": 150.0, "unit": "mentions"},
    {"section": "top_keywords", "metric": "mean_lexicon_tone", "dimension": "unigram:lava", "period": None, "value": 0.65, "unit": "score"},
    {"section": "top_keywords", "metric": "frequency", "dimension": "bigram:ash_cloud", "period": None, "value": 45.0, "unit": "mentions"},
    {"section": "top_keywords", "metric": "mean_lexicon_tone", "dimension": "bigram:ash_cloud", "period": None, "value": 0.72, "unit": "score"},
    {"section": "source_comparison", "metric": "mean_text_length", "dimension": "usgs_hans_notices_api", "period": None, "value": 2400.0, "unit": "chars"},
    {"section": "source_comparison", "metric": "lexicon_mean", "dimension": "usgs_hans_notices_api", "period": None, "value": 0.60, "unit": "score"},
    {"section": "source_comparison", "metric": "vader_mean", "dimension": "usgs_hans_notices_api", "period": None, "value": 0.15, "unit": "score"},
    {"section": "volume_activity", "metric": "records", "dimension": "usgs_hans_notices_api", "period": date(2026, 11, 9), "value": 18.0, "unit": "records"},
    {"section": "volume_activity", "metric": "records", "dimension": "usgs_hans_notices_api", "period": date(2026, 11, 16), "value": 24.0, "unit": "records"},
    {"section": "narrative_highlights", "metric": "elevated_volcanoes_count", "dimension": "ALL", "period": None, "value": 3.0, "unit": "volcanoes"},
    {"section": "narrative_highlights", "metric": "latest_week_tone", "dimension": "ALL", "period": None, "value": -0.10, "unit": "score"},
    {"section": "narrative_highlights", "metric": "tone_wow_delta", "dimension": "ALL", "period": None, "value": -0.30, "unit": "score"},
    {"section": "narrative_highlights", "metric": "top_escalating_volcano", "dimension": "Kilauea", "period": None, "value": 0.85, "unit": "score"},
]


@pytest.fixture
def full_story_folder(tmp_path):
    write_gold(tmp_path, "storytelling", "20261127_230000", FULL_STORYTELLING_ROWS)
    return tmp_path


def test_narrative_card_renders_insights_and_fallback(full_story_folder, tmp_path):
    snap = load_gold("storytelling", full_story_folder)
    card_text = str(storytelling_app.build_narrative_card(snap))
    assert "Unrest Fading" in card_text
    assert "Kilauea" in card_text

    empty_snap = load_gold("storytelling", tmp_path / "empty")
    empty_text = str(storytelling_app.build_narrative_card(empty_snap))
    assert "No Gold storytelling summary available yet" in empty_text


def test_all_storytelling_figures_have_titles_and_annotations(full_story_folder):
    snap = load_gold("storytelling", full_story_folder)

    donut = storytelling_app.tone_donut_figure(snap)
    assert "Distribution" in donut.layout.title.text
    assert len(donut.data[0].values) == 3

    trend = storytelling_app.weekly_tone_figure(snap, "ALL")
    assert "Weekly Activity Tone Trajectory" in trend.layout.title.text
    assert trend.layout.xaxis.title.text == "Week (UTC)"
    assert "Activity tone score" in trend.layout.yaxis.title.text
    assert len(trend.data) == 2  # lexicon + VADER

    kw_uni = storytelling_app.top_keywords_figure(snap, "unigram")
    assert "Keywords (Unigrams)" in kw_uni.layout.title.text
    assert kw_uni.layout.xaxis.title.text == "Frequency in notice summaries (mentions)"

    kw_bi = storytelling_app.top_keywords_figure(snap, "bigram")
    assert "Phrases (Bigrams)" in kw_bi.layout.title.text

    src_cmp = storytelling_app.source_comparison_figure(snap)
    assert "Comparison" in src_cmp.layout.title.text
    assert any("Source:" in a.text for a in src_cmp.layout.annotations)

    vol = storytelling_app.volume_activity_figure(snap, "usgs_hans_notices_api")
    assert "Weekly Monitoring Volume" in vol.layout.title.text
    assert vol.layout.yaxis.title.text == "Ingested records (count)"


def test_storytelling_layout_contains_filters_and_cards(full_story_folder):
    layout_str = str(storytelling_app.create_app(full_story_folder).layout())
    for component_id in [
        "narrative-card",
        "tone-donut",
        "tone-trend",
        "volume-activity",
        "top-keywords",
        "source-comparison",
        "tone-scope-filter",
        "volume-source-filter",
        "keyword-type-filter",
    ]:
        assert component_id in layout_str
