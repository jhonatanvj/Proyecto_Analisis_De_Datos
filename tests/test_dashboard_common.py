"""Shared dashboard layer: Gold loader, theme and layout pieces (no browser, no server)."""
from __future__ import annotations

import pandas as pd
import pytest

pytest.importorskip("dash")   # the Airflow image has no Dash; these run on the host

from dashboard.common import gold  # noqa: E402
from dashboard.common.gold import GOLD_COLUMNS, latest_gold_file, load_gold  # noqa: E402
from dashboard_helpers import GOVERNANCE_ROWS, write_gold  # noqa: E402


def test_latest_file_ignores_other_kinds_and_spark_tmp(tmp_path):
    write_gold(tmp_path, "governance", "20261101_000000", GOVERNANCE_ROWS)
    write_gold(tmp_path, "governance", "20261124_233802", GOVERNANCE_ROWS)
    write_gold(tmp_path, "storytelling", "20261130_000000", GOVERNANCE_ROWS)
    (tmp_path / ".governance_20261201_000000.tmp").mkdir()
    assert latest_gold_file("governance", tmp_path).name == "governance_20261124_233802.parquet"


def test_missing_folder_gives_empty_snapshot(tmp_path):
    snap = load_gold("governance", tmp_path / "nope")
    assert snap.path is None and snap.generated_utc is None
    assert list(snap.frame.columns) == GOLD_COLUMNS and snap.frame.empty
    assert snap.section("null_rate").empty


def test_section_filters_rows_and_generated_utc_is_read(tmp_path):
    write_gold(tmp_path, "governance", "20261124_233802", GOVERNANCE_ROWS)
    snap = load_gold("governance", tmp_path)
    assert len(snap.section("volume_total")) == 2
    assert snap.generated_utc == pd.Timestamp("2026-11-24 23:38:02")


def test_newer_file_is_picked_up_and_unchanged_file_is_cached(tmp_path, monkeypatch):
    write_gold(tmp_path, "governance", "20261124_233802", GOVERNANCE_ROWS)
    first = load_gold("governance", tmp_path)
    calls = []
    real_read = gold.pd.read_parquet
    monkeypatch.setattr(gold.pd, "read_parquet", lambda *a, **k: calls.append(a) or real_read(*a, **k))
    assert load_gold("governance", tmp_path) is first and calls == []
    write_gold(tmp_path, "governance", "20261125_000000", GOVERNANCE_ROWS)
    assert load_gold("governance", tmp_path).path.name == "governance_20261125_000000.parquet"
    assert len(calls) == 1


def test_unknown_kind_is_rejected(tmp_path):
    with pytest.raises(ValueError):
        load_gold("silver", tmp_path)


def test_style_figure_sets_title_axes_and_source():
    import plotly.graph_objects as go

    from dashboard.common.theme import style_figure

    fig = style_figure(go.Figure(go.Bar(x=["a"], y=[1])), "Records per topic", "Topic", "Records", "Gold governance")
    assert fig.layout.title.text == "Records per topic"
    assert fig.layout.xaxis.title.text == "Topic" and fig.layout.yaxis.title.text == "Records"
    assert any("Source: Gold governance" in a.text for a in fig.layout.annotations)


def test_tone_colours_are_green_grey_red():
    from dashboard.common.theme import TONE_COLORS

    assert TONE_COLORS == {"calming": "#009E73", "stable": "#999999", "escalating": "#D55E00"}


def test_last_updated_text(tmp_path):
    from dashboard.common.layout import last_updated_text

    assert last_updated_text(load_gold("governance", tmp_path)) == "Data last updated: no Gold data yet"
    write_gold(tmp_path, "governance", "20261124_233802", GOVERNANCE_ROWS)
    assert last_updated_text(load_gold("governance", tmp_path)) == "Data last updated: 2026-11-24 23:38 UTC"


def test_empty_figure_shows_message():
    from dashboard.common.layout import empty_figure

    fig = empty_figure("No Gold data yet")
    assert fig.data == () and "No Gold data yet" in fig.layout.annotations[0].text


def test_source_note_sits_inside_the_bottom_margin_at_any_height():
    """The note is placed in pixels below the axis, so it is never clipped and never moves with the height."""
    import plotly.graph_objects as go
    import plotly.io as pio

    from dashboard.common.theme import TEMPLATE, style_figure

    fig = style_figure(go.Figure(go.Bar(x=["a"], y=[1])), "T", "X", "Y", "Gold")
    note = next(a for a in fig.layout.annotations if a.text.startswith("Source:"))
    bottom_margin = pio.templates[TEMPLATE].layout.margin.b
    assert note.yref == "paper" and note.y == 0 and note.yanchor == "top"
    # below the tick labels and the axis title, and the whole 11 px line fits in the margin
    assert note.yshift <= -50 and -note.yshift + 2 * note.font.size <= bottom_margin


def test_no_rows_figure_tells_missing_gold_from_missing_section(tmp_path):
    from dashboard.common.layout import no_rows_figure

    assert "No Gold data yet" in no_rows_figure(load_gold("governance", tmp_path), "null_rate").layout.annotations[0].text
    write_gold(tmp_path, "governance", "20261124_233802", GOVERNANCE_ROWS)
    text = no_rows_figure(load_gold("governance", tmp_path), "outlier_rate").layout.annotations[0].text
    assert "outlier_rate" in text and "No Gold data yet" not in text


def test_axes_grow_the_margins_for_long_labels():
    """Long category names (e.g. volcano_updates on a horizontal bar) must not be cut at the left edge."""
    import plotly.io as pio

    from dashboard.common.theme import TEMPLATE

    layout = pio.templates[TEMPLATE].layout
    assert layout.xaxis.automargin is True and layout.yaxis.automargin is True


def test_rewritten_file_with_the_same_name_is_read_again(tmp_path):
    import os

    path = write_gold(tmp_path, "governance", "20261124_233802", GOVERNANCE_ROWS)
    assert len(load_gold("governance", tmp_path).frame) == 3
    write_gold(tmp_path, "governance", "20261124_233802", GOVERNANCE_ROWS[:1])
    stat = path.stat()
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 10**9))   # filesystems with coarse mtimes
    assert len(load_gold("governance", tmp_path).frame) == 1


def test_unreadable_newest_file_falls_back_to_the_previous_one(tmp_path, caplog):
    write_gold(tmp_path, "governance", "20261124_233802", GOVERNANCE_ROWS)
    (tmp_path / "governance_20261125_000000.parquet").write_bytes(b"not a parquet file")
    snap = load_gold("governance", tmp_path)
    assert snap.path.name == "governance_20261124_233802.parquet" and len(snap.frame) == 3
    assert "governance_20261125_000000.parquet" in caplog.text


def test_file_without_a_contract_column_is_skipped(tmp_path):
    write_gold(tmp_path, "governance", "20261124_233802", GOVERNANCE_ROWS)
    newer = write_gold(tmp_path, "governance", "20261125_000000", GOVERNANCE_ROWS)
    pd.read_parquet(newer).drop(columns=["unit"]).to_parquet(newer, index=False)
    assert load_gold("governance", tmp_path).path.name == "governance_20261124_233802.parquet"


def test_only_unreadable_files_give_an_empty_snapshot(tmp_path):
    (tmp_path / "governance_20261125_000000.parquet").write_bytes(b"broken")
    snap = load_gold("governance", tmp_path)
    assert snap.path is None and snap.frame.empty
