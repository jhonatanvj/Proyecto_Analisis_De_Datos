"""Governance dashboard (Workshop 4, Part 2): every component on a small Gold file with known values."""
from __future__ import annotations

from datetime import date

import pytest

pytest.importorskip("dash")   # the Airflow image has no Dash; these run on the host

from dashboard import governance_app as app_module  # noqa: E402
from dashboard.common.gold import load_gold  # noqa: E402
from dashboard_helpers import write_gold  # noqa: E402

STAMP = "20261127_120000"
W1, W2 = date(2026, 11, 9), date(2026, 11, 16)


def _r(section, metric, dimension, value, unit, period=None):
    return {"section": section, "metric": metric, "dimension": dimension, "period": period, "value": value,
            "unit": unit}


ROWS = [
    _r("volume_total", "records", "earthquakes", 1000.0, "records"),
    _r("volume_total", "records", "quarantine", 20.0, "records"),
    _r("null_rate", "pct_null", "eruptions.vei", 37.5, "%"),
    _r("null_rate", "pct_null", "notices.region", 11.4, "%"),
    _r("null_rate", "pct_null", "notices.record_id", 0.0, "%"),
    _r("outlier_rate", "pct_outlier", "earthquakes.mag", 3.2, "%"),
    _r("outlier_rate", "pct_outlier", "earthquakes.depth_km", 9.4, "%"),
    _r("outlier_rate", "pct_outlier_silver_flags", "earthquakes.mag", 3.8, "%"),
    _r("outlier_rate", "pct_outlier_silver_flags", "earthquakes.depth_km", 5.7, "%"),
    _r("volume_weekly", "records", "earthquakes", 30.0, "records", W1),
    _r("volume_weekly", "records_by_source", "usgs_feed", 10.0, "records", W1),
    _r("volume_weekly", "records_by_source", "usgs_feed", 12.0, "records", W2),
    _r("volume_weekly", "records_by_source", "usgs_fdsn", 20.0, "records", W1),
    _r("freshness", "hours_since_last", "usgs_feed", 0.5, "hours"),
    _r("freshness", "hours_since_last", "gvp_volcanoes", 260.0, "hours"),
    _r("data_health", "records_in_silver", "usgs_feed", 1000.0, "records"),
    _r("data_health", "rejected_records", "usgs_feed", 20.0, "records"),
    _r("data_health", "last_file_utc", "usgs_feed", 1795780800.0, "unix_s"),     # 2026-11-27 12:00 UTC
    _r("data_health", "watermark_age_hours", "usgs_feed", 0.7, "hours"),
    _r("data_health", "records_in_silver", "gvp_volcanoes", 165.0, "records"),
    _r("data_health", "last_file_utc", "gvp_volcanoes", 1794879000.0, "unix_s"),
    # kpi_summary as gold/governance.py writes it: value, threshold_<rule>, status
    _r("kpi_summary", "records_in_silver", "volume", 1000.0, "records"),
    _r("kpi_summary", "mean_pct_null", "null_rate", 3.6, "%"),
    _r("kpi_summary", "threshold_max", "null_rate", 5.0, "%"),
    _r("kpi_summary", "status", "null_rate", 1.0, "pass=1"),
    _r("kpi_summary", "pct_exact_copies", "duplicate_rate", 0.089, "%"),
    _r("kpi_summary", "threshold_max", "duplicate_rate", 0.0, "%"),
    _r("kpi_summary", "status", "duplicate_rate", 0.0, "pass=1"),
    _r("kpi_summary", "min_pct_files_conforming", "schema_compliance", 100.0, "%"),
    _r("kpi_summary", "threshold_min", "schema_compliance", 100.0, "%"),
    _r("kpi_summary", "status", "schema_compliance", 1.0, "pass=1"),
    _r("kpi_summary", "max_pct_outlier", "outlier_rate", 9.4, "%"),
    _r("kpi_summary", "threshold_max", "outlier_rate", 15.0, "%"),
    _r("kpi_summary", "status", "outlier_rate", 1.0, "pass=1"),
]


@pytest.fixture
def snapshot(tmp_path):
    write_gold(tmp_path, "governance", STAMP, ROWS)
    return load_gold("governance", tmp_path)


def _has_source_note(fig):
    return any("Source:" in a.text for a in fig.layout.annotations)


def test_kpi_table_pivots_value_threshold_and_status(snapshot):
    table = app_module.kpi_table(snapshot).set_index("kpi")
    assert list(table.index[:3]) == ["volume", "null_rate", "duplicate_rate"]        # catalogue order
    assert table.at["null_rate", "threshold"] == 5.0 and table.at["null_rate", "rule"] == "max"
    assert table.at["duplicate_rate", "status"] == 0.0
    assert table.at["schema_compliance", "rule"] == "min"


def test_kpi_cards_show_value_badge_and_threshold(snapshot):
    text = str(app_module.kpi_cards(snapshot))
    assert "Total records" in text and "1,000" in text
    assert "0.089 %" in text and "Warning" in text and "Pass" in text and "≤ 5.0 %" in text
    assert "Quarantine rate" in text and "n/a" in text            # KPI missing from this Gold file


def test_null_rate_bars_are_sorted_and_filtered_by_topic(snapshot):
    fig = app_module.null_rate_figure(snapshot)
    assert list(fig.data[0].y) == ["notices.region", "eruptions.vei"]     # 0 % field dropped, worst on top
    assert fig.layout.xaxis.title.text == "Empty values (%)" and _has_source_note(fig)
    assert list(app_module.null_rate_figure(snapshot, ["notices"]).data[0].y) == ["notices.region"]


def test_outlier_figure_compares_gold_and_silver_with_the_threshold(snapshot):
    fig = app_module.outlier_figure(snapshot)
    assert [t.name for t in fig.data] == ["Per-volcano fences (KPI)", "Silver flags, per file"]
    assert list(fig.data[0].x) == ["Magnitude", "Depth (km)"] and list(fig.data[0].y) == [3.2, 9.4]
    assert fig.layout.yaxis.title.text == "Outliers (% of records)"
    assert any(s.y0 == 15.0 for s in fig.layout.shapes)              # dashed threshold line


def test_volume_figure_has_one_line_per_source_and_filters(snapshot):
    fig = app_module.volume_figure(snapshot)
    assert {t.name for t in fig.data} == {"usgs_feed", "usgs_fdsn"} and fig.layout.yaxis.type == "log"
    assert "source" in fig.layout.title.text
    assert [t.name for t in app_module.volume_figure(snapshot, ["usgs_feed"]).data] == ["usgs_feed"]


def test_volume_figure_falls_back_to_topics_in_older_gold(tmp_path):
    write_gold(tmp_path, "governance", STAMP, [r for r in ROWS if r["metric"] != "records_by_source"])
    fig = app_module.volume_figure(load_gold("governance", tmp_path))
    assert [t.name for t in fig.data] == ["earthquakes"] and "topic" in fig.layout.title.text


def test_quality_table_lists_every_kpi_with_status(snapshot):
    text = str(app_module.quality_table(snapshot))
    for label in ["Total records", "Null rate", "Duplicate rate", "Schema compliance", "Outlier rate"]:
        assert label in text
    assert "≥ 100.0 %" in text and "Info" in text                 # volume has no threshold


def test_health_table_one_row_per_source_and_source_filter(snapshot):
    text = str(app_module.health_table(snapshot))
    assert "usgs_feed" in text and "gvp_volcanoes" in text and "2026-11-27 12:00" in text
    assert "0.7 h" in text and "260.0 h" in text
    filtered = str(app_module.health_table(snapshot, ["gvp_volcanoes"]))
    assert "gvp_volcanoes" in filtered and "usgs_feed" not in filtered


def test_filter_options_come_from_gold(snapshot):
    sources, topics = app_module.filter_options(snapshot)
    assert sources == ["gvp_volcanoes", "usgs_fdsn", "usgs_feed"] and topics == ["eruptions", "notices"]


def test_every_component_handles_an_empty_gold_folder(tmp_path):
    empty = load_gold("governance", tmp_path / "none")
    for fig in (app_module.null_rate_figure(empty), app_module.outlier_figure(empty),
                app_module.volume_figure(empty), app_module.records_per_topic_figure(empty)):
        assert fig.data == () and "No Gold data yet" in fig.layout.annotations[0].text
    assert "run the gold_processing DAG" in str(app_module.quality_table(empty))
    assert "run the gold_processing DAG" in str(app_module.health_table(empty))


def test_layout_has_every_component_and_the_callback_fills_them(tmp_path):
    write_gold(tmp_path, "governance", STAMP, ROWS)
    app = app_module.create_app(tmp_path)
    layout = str(app.layout())
    for component in ["source-filter", "topic-filter", "kpi-cards", "null-rate", "outlier-rate",
                      "volume-over-time", "records-per-topic", "quality-summary", "data-health"]:
        assert component in layout
    callback = next(iter(app.callback_map.values()))["callback"]
    outputs = callback.__wrapped__(None, None, None, None)
    assert len(outputs) == 8 and outputs[0] == "Data last updated: 2026-11-27 12:00 UTC"
