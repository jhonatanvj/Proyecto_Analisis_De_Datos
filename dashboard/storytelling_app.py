"""Storytelling dashboard (Workshop 4), http://localhost:8051

Which US volcanoes show unusual activity, and is it building or fading? Written for the
emergency-management analyst of Workshop 1, read only from the latest
datalake_gold/storytelling_*.parquet.

Run: python -m dashboard.storytelling_app   (or the storytelling-dashboard service in docker compose)

Part 3 adds its charts the same way as weekly_tone_figure: a figure builder that takes a
GoldSnapshot, a chart_card() in the layout and one more Output in refresh().
"""
from __future__ import annotations

import os
from pathlib import Path

import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import Dash, Input, Output

from dashboard import indicator_panel
from dashboard.common.gold import GoldSnapshot, load_gold
from dashboard.common.layout import chart_card, header, last_updated_text, no_rows_figure, refresh_controls
from dashboard.common.theme import CATEGORICAL, style_figure

KIND = "storytelling"
TITLE = "Volcano Activity Story"
ANGLE = "Which US volcanoes show unusual activity, and is it building or fading?"
SOURCE = "USGS HANS notices via the Gold layer (storytelling_*.parquet)"
DEFAULT_PORT = 8051


def weekly_tone_figure(snapshot: GoldSnapshot) -> go.Figure:
    """Example chart: weekly activity tone of the USGS notices for all US volcanoes together."""
    tone = snapshot.section("tone_weekly")
    rows = tone[(tone["metric"] == "lexicon_mean") & (tone["dimension"] == "ALL")].sort_values("period")
    if rows.empty:
        return no_rows_figure(snapshot, "tone_weekly")
    fig = go.Figure(go.Scatter(x=rows["period"], y=rows["value"], mode="lines",
                               line={"color": CATEGORICAL[0], "width": 2},
                               hovertemplate="Week of %{x|%b %d, %Y}: %{y:.2f}<extra></extra>"))
    fig.add_hline(y=0, line={"color": "#999999", "dash": "dot"})
    fig.update_yaxes(range=[-1, 1])
    return style_figure(fig, "Tone of the USGS notices, week by week",
                        "Week", "Activity tone (-1 calming, +1 escalating)", SOURCE)


def create_app(gold_folder: Path | None = None) -> Dash:
    app = Dash(__name__, title=TITLE, external_stylesheets=[dbc.themes.FLATLY])

    def page():
        # a function, so every page load (or browser reload) reads the newest Gold file
        snapshot = load_gold(KIND, gold_folder)
        return dbc.Container([
            header(TITLE, ANGLE, snapshot),
            dbc.Container([
                refresh_controls(),
                # Part 3 (storytelling): narrative card, tone donut, tone trend, top keywords,
                # API vs. scraping comparison, volume activity.
                dbc.Row(dbc.Col(chart_card("weekly-tone"), lg=12)),
                # Part 1: Volcano Activity Indicator (map, ranking by region, history per volcano)
                indicator_panel.layout(),
            ], fluid=True),
        ], fluid=True, className="px-0")

    app.layout = page
    indicator_panel.register_callbacks(app, gold_folder)

    @app.callback(Output("last-updated", "children"), Output("weekly-tone", "figure"),
                  Input("refresh-button", "n_clicks"), Input("refresh-interval", "n_intervals"))
    def refresh(_clicks, _ticks):
        current = load_gold(KIND, gold_folder)
        return last_updated_text(current), weekly_tone_figure(current)

    return app


def main() -> None:
    create_app().run(host=os.getenv("DASH_HOST", "127.0.0.1"),
                     port=int(os.getenv("STORYTELLING_PORT", DEFAULT_PORT)), debug=False)


if __name__ == "__main__":
    main()
