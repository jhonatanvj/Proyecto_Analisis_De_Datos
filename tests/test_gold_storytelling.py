"""Unit tests for Workshop 3 Part 3: activity tone (lexicon vs. VADER) and storytelling Gold."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from test_gold import spark  # noqa: F401


def test_lexicon_distinguishes_escalation_and_deescalation():
    from volcano_pipeline.gold.tone import lexicon_score

    escalating = "Explosive ash plume and strong volcanic tremor with lava fountaining."
    calming = "Seismicity has decreased and activity subsided to normal background levels."
    negated = "There is no eruption and no ash emission observed today."

    assert lexicon_score(escalating) > 0.5
    assert lexicon_score(calming) < -0.5
    assert lexicon_score(negated) < 0.0
    assert lexicon_score("") == 0.0
    assert lexicon_score(None) == 0.0


def test_vader_score_runs_on_volcano_text():
    pytest.importorskip("vaderSentiment")
    from volcano_pipeline.gold.tone import vader_score

    score = vader_score("Volcanic unrest remains elevated with minor steam emissions.")
    assert -1.0 <= score <= 1.0
    assert vader_score(None) == 0.0


def test_build_storytelling_all_sections(spark):
    pytest.importorskip("vaderSentiment")
    from volcano_pipeline.gold.contract import COLUMNS
    from volcano_pipeline.gold.storytelling import build_storytelling

    when = datetime(2026, 11, 23, 3, 0, tzinfo=timezone.utc)

    notices = spark.createDataFrame(
        [
            (
                "n1",
                datetime(2026, 11, 10, 12, 0),
                "HVO",
                "WU",
                332010,
                "Kilauea",
                "Hawaii",
                "WATCH",
                3,
                "ORANGE",
                "Lava eruption and tremor increased at the summit.",
                ["lava", "eruption", "tremor", "increased", "summit"],
                "Full notice text describing lava eruption and seismic tremor.",
                datetime(2026, 11, 10, 12, 5),
            ),
            (
                "n2",
                datetime(2026, 11, 17, 12, 0),
                "HVO",
                "WU",
                332010,
                "Kilauea",
                "Hawaii",
                "ADVISORY",
                2,
                "YELLOW",
                "Eruption paused and seismicity decreased to low background.",
                ["eruption", "paused", "seismicity", "decreased", "background"],
                "Full notice text describing paused eruption and decreased seismicity.",
                datetime(2026, 11, 17, 12, 5),
            ),
        ],
        """record_id string, sent_time timestamp, observatory string, notice_type string,
           vnum int, volcano_name string, region string, alert_level string,
           alert_level_ord int, color_code string, summary_clean string,
           summary_tokens array<string>, text_clean string, processed_utc timestamp""",
    )

    updates = spark.createDataFrame(
        [
            (
                "u1",
                "Kilauea eruption update with ash plume and elevated tremor.",
                datetime(2026, 11, 17, 13, 0),
            )
        ],
        "record_id string, text_clean string, processed_utc timestamp",
    )

    alerts = spark.createDataFrame(
        [
            (332010, 332010, "WATCH", "ORANGE", datetime(2026, 11, 17, 12, 5)),
            (311240, 311240, "NORMAL", "GREEN", datetime(2026, 11, 17, 12, 5)),
        ],
        "record_id int, vnum int, alert_level string, color_code string, processed_utc timestamp",
    )

    earthquakes = spark.createDataFrame(
        [
            ("eq1", datetime(2026, 11, 10, 1, 0), 332010, datetime(2026, 11, 10, 2, 0)),
            ("eq2", datetime(2026, 11, 17, 1, 0), 332010, datetime(2026, 11, 17, 2, 0)),
            ("eq3", datetime(2026, 11, 17, 3, 0), 332010, datetime(2026, 11, 17, 4, 0)),
        ],
        "record_id string, event_time timestamp, nearest_vnum int, processed_utc timestamp",
    )

    silver = {
        "notices": notices,
        "volcano_updates": updates,
        "alert_notices": alerts,
        "earthquakes": earthquakes,
    }

    gold_df = build_storytelling(spark, silver, when)
    assert gold_df.columns == COLUMNS

    sections = {r["section"] for r in gold_df.select("section").distinct().collect()}
    assert sections == {
        "earthquakes_weekly",
        "tone_weekly",
        "top_keywords",
        "source_comparison",
        "earthquakes_baseline",
        "alerts_breakdown",
        # Workshop 4, Part 3: sections behind the storytelling dashboard
        "tone_distribution",
        "volume_activity",
        "narrative_highlights",
    }
