"""Both dashboard skeletons build, show the header and render their example chart, also with no Gold."""
from __future__ import annotations

import pytest

pytest.importorskip("dash")   # the Airflow image has no Dash; these run on the host

from dashboard import governance_app, storytelling_app  # noqa: E402
from dashboard.common.gold import load_gold  # noqa: E402
from dashboard_helpers import GOVERNANCE_ROWS, STORYTELLING_ROWS, write_gold  # noqa: E402


@pytest.fixture
def gold_folder(tmp_path):
    write_gold(tmp_path, "governance", "20261124_233802", GOVERNANCE_ROWS)
    write_gold(tmp_path, "storytelling", "20261124_233802", STORYTELLING_ROWS)
    return tmp_path


@pytest.mark.parametrize("module", [governance_app, storytelling_app])
def test_app_builds_with_header(module, gold_folder):
    text = str(module.create_app(gold_folder).layout())
    assert "Team 6" in text and "Data last updated: 2026-11-24 23:38 UTC" in text


@pytest.mark.parametrize("module", [governance_app, storytelling_app])
def test_app_starts_without_gold(module, tmp_path):
    assert "no Gold data yet" in str(module.create_app(tmp_path / "empty").layout())


@pytest.mark.parametrize("module", [governance_app, storytelling_app])
def test_page_load_reads_gold_written_after_the_app_started(module, tmp_path):
    app = module.create_app(tmp_path)
    write_gold(tmp_path, module.KIND, "20261124_233802",
               GOVERNANCE_ROWS if module is governance_app else STORYTELLING_ROWS)
    assert "Data last updated: 2026-11-24 23:38 UTC" in str(app.layout())


def test_records_per_topic_figure(gold_folder):
    fig = governance_app.records_per_topic_figure(load_gold("governance", gold_folder))
    assert fig.layout.xaxis.title.text == "Records" and len(fig.data[0].y) == 2


def test_weekly_tone_figure_uses_the_all_volcanoes_series(gold_folder):
    fig = storytelling_app.weekly_tone_figure(load_gold("storytelling", gold_folder))
    assert list(fig.data[0].y) == [0.2, -0.1]
    assert any("Source:" in a.text for a in fig.layout.annotations)


def test_weekly_tone_figure_without_section_is_empty(tmp_path):
    write_gold(tmp_path, "storytelling", "20261124_233950", GOVERNANCE_ROWS)   # no tone_weekly rows
    fig = storytelling_app.weekly_tone_figure(load_gold("storytelling", tmp_path))
    assert fig.data == () and "tone_weekly" in fig.layout.annotations[0].text
