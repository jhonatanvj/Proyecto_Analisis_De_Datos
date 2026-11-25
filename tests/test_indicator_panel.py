"""Volcano Activity Indicator panel of the storytelling dashboard (figure builders and layout)."""
from __future__ import annotations

import pytest

pytest.importorskip("dash")   # the Airflow image has no Dash; these run on the host

from dashboard import indicator_panel as panel  # noqa: E402
from dashboard import storytelling_app  # noqa: E402
from dashboard.common.gold import load_gold  # noqa: E402
from dashboard.common.theme import ALERT_LEVEL_COLORS, VAI_LEVEL_COLORS  # noqa: E402
from dashboard_helpers import INDICATOR_ROWS, STORYTELLING_ROWS, write_gold  # noqa: E402


@pytest.fixture
def snapshot(tmp_path):
    write_gold(tmp_path, "storytelling", "20261125_030000", STORYTELLING_ROWS + INDICATOR_ROWS)
    return load_gold("storytelling", tmp_path)


@pytest.fixture
def old_snapshot(tmp_path):
    """A storytelling file from before the indicator existed."""
    write_gold(tmp_path, "storytelling", "20261124_233950", STORYTELLING_ROWS)
    return load_gold("storytelling", tmp_path)


def test_indicator_table_joins_map_and_vai(snapshot):
    table = panel.indicator_table(snapshot).set_index("vnum")
    assert table.loc["311120", "name"] == "Great Sitkin" and table.loc["311120", "region"] == "Alaska"
    assert table.loc["311120", "alert_level"] == "WATCH" and table.loc["311120", "vai_level"] == "ELEVATED"
    assert table.loc["244001", "alert_level"] == "UNASSIGNED"
    assert table.loc["332010", "components"] == ["official alert level", "eruptive history"]


def test_ranking_filters_by_region_and_colours_by_level(snapshot):
    table = panel.indicator_table(snapshot)
    fig = panel.ranking_figure(table, "Alaska")
    assert list(fig.data[0].y) == ["Great Sitkin"] and fig.data[0].marker.color == VAI_LEVEL_COLORS["ELEVATED"]
    assert fig.layout.xaxis.title.text.startswith("Volcano Activity Indicator")
    everyone = panel.ranking_figure(table, panel.ALL_REGIONS)
    # categories are drawn bottom to top, so the highest VAI comes last
    assert list(everyone.layout.yaxis.categoryarray) == ["Great Sitkin", "Kilauea"]


def test_map_uses_the_alert_colours(snapshot):
    fig = panel.map_figure(panel.indicator_table(snapshot))
    colours = {t.name: t.marker.color for t in fig.data}
    assert colours["WATCH"] == ALERT_LEVEL_COLORS["WATCH"] and colours["UNASSIGNED"] == ALERT_LEVEL_COLORS["UNASSIGNED"]
    assert fig.data[0].type == "scattermap"
    assert any("Source:" in a.text for a in fig.layout.annotations)


def test_history_has_the_weekly_line_and_the_eruptions(snapshot):
    fig = panel.history_figure(snapshot, "311120")
    line, eruptions = fig.data[0], fig.data[2]
    assert list(line.y) == [20.0, 45.0, 60.0] and len(eruptions.x) == 1
    assert "Great Sitkin" in fig.layout.title.text and fig.layout.yaxis.range == (0, 100)


def test_history_without_weekly_rows_explains_why(snapshot):
    fig = panel.history_figure(snapshot, "332010")
    assert fig.data == () and "20 earthquakes" in fig.layout.annotations[0].text


def test_components_text_lists_what_was_used(snapshot):
    text = panel.components_text(panel.indicator_table(snapshot), "311120")
    assert "72.2" in text and "ELEVATED" in text and "official alert level" in text
    assert "eruptive history" in text.split("Not available")[1]


def test_pick_volcano_keeps_a_valid_choice_and_follows_clicks(snapshot):
    table = panel.indicator_table(snapshot)
    assert panel.pick_volcano(table, panel.ALL_REGIONS, None, None) == "332010"       # top of the ranking
    assert panel.pick_volcano(table, panel.ALL_REGIONS, "311120", None) == "311120"
    # changing the region moves the choice into it
    assert panel.pick_volcano(table, "Alaska", "332010", None, region_changed=True) == "311120"
    assert panel.pick_volcano(table, panel.ALL_REGIONS, "332010", "311120") == "311120"


def test_refresh_does_not_undo_a_click_outside_the_region(snapshot):
    """Region Alaska, the user clicks Kilauea on the map: the 5-minute refresh must keep Kilauea."""
    table = panel.indicator_table(snapshot)
    assert panel.pick_volcano(table, "Alaska", "332010", None, region_changed=False) == "332010"


def test_clicking_a_volcano_without_indicator_keeps_the_choice(snapshot):
    table = panel.indicator_table(snapshot)
    assert panel.pick_volcano(table, panel.ALL_REGIONS, "311120", "244001") == "311120"


def test_map_hover_says_no_indicator_instead_of_nan(snapshot):
    fig = panel.map_figure(panel.indicator_table(snapshot))
    unassigned = next(t for t in fig.data if t.name == "UNASSIGNED")
    assert [row[3] for row in unassigned.customdata] == ["no indicator"]


def test_ranking_shows_how_many_inputs_each_score_uses(snapshot):
    table = panel.indicator_table(snapshot)
    assert table.set_index("vnum").loc["311120", "components_used"] == 2
    fig = panel.ranking_figure(table, panel.ALL_REGIONS)
    assert all("input" in t.hovertemplate and "earthquakes" in t.hovertemplate for t in fig.data)
    assert "lighter bars" in fig.layout.title.text.lower()


def test_single_input_scores_are_drawn_lighter(tmp_path):
    one_input = [dict(r) for r in INDICATOR_ROWS]
    for r in one_input:
        if r["dimension"] == "332010" and r["metric"] == "components_used":
            r["value"] = 1.0
    write_gold(tmp_path, "storytelling", "20261125_030000", one_input)
    fig = panel.ranking_figure(panel.indicator_table(load_gold("storytelling", tmp_path)), panel.ALL_REGIONS)
    opacity = {y: o for t in fig.data for y, o in zip(t.y, t.marker.opacity)}
    assert opacity["Kilauea"] < opacity["Great Sitkin"] == 1.0


def test_every_builder_survives_a_file_without_the_indicator(old_snapshot):
    table = panel.indicator_table(old_snapshot)
    assert table.empty
    for fig in (panel.map_figure(table), panel.ranking_figure(table, panel.ALL_REGIONS),
                panel.history_figure(old_snapshot, None)):
        assert fig.data == () and fig.layout.annotations
    assert panel.pick_volcano(table, panel.ALL_REGIONS, None, None) is None


def test_storytelling_page_shows_the_panel_and_the_disclaimer(snapshot):
    text = str(storytelling_app.create_app(snapshot.path.parent).layout())
    assert "vai-region" in text and "volcano-map" in text
    assert "not an eruption forecast" in text


def test_ranking_shows_every_name_and_explains_the_colours(snapshot):
    fig = panel.ranking_figure(panel.indicator_table(snapshot), panel.ALL_REGIONS)
    assert fig.layout.yaxis.dtick == 1                      # Plotly would skip names on a crowded axis
    assert {t.name for t in fig.data} == {"ELEVATED", "HIGH"} and fig.layout.showlegend is not False


def test_history_shows_a_four_week_average_over_the_weekly_values(snapshot):
    fig = panel.history_figure(snapshot, "311120")
    names = [t.name for t in fig.data]
    assert names[:2] == ["Weekly value", "4-week average"]
    assert list(fig.data[1].y) == pytest.approx([20.0, 32.5, 125 / 3])


def test_map_legend_sits_inside_the_map(snapshot):
    legend = panel.map_figure(panel.indicator_table(snapshot)).layout.legend
    assert legend.yanchor == "top" and legend.y <= 1
