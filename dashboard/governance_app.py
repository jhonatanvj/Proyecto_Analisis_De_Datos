"""Governance dashboard (Workshop 4), http://localhost:8050

Data quality of the volcano pipeline for the data team, read only from the latest
datalake_gold/governance_*.parquet.

Run: python -m dashboard.governance_app   (or the governance-dashboard service in docker compose)

Part 2 adds its charts the same way as records_per_topic_figure: a figure builder that takes a
GoldSnapshot, a chart_card() in the layout and one more Output in refresh().
"""
from __future__ import annotations

import os
from pathlib import Path

import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import Dash, Input, Output

from dashboard.common.gold import GoldSnapshot, load_gold
from dashboard.common.layout import chart_card, header, last_updated_text, no_rows_figure, refresh_controls
from dashboard.common.theme import CATEGORICAL, style_figure

KIND = "governance"
TITLE = "Data Governance Dashboard"
ANGLE = "Data quality of the volcano pipeline, for the data team"
SOURCE = "USGS / GVP via the Gold layer (governance_*.parquet)"
DEFAULT_PORT = 8050


def records_per_topic_figure(snapshot: GoldSnapshot) -> go.Figure:
    """Example chart: records stored per Silver topic (section volume_total)."""
    rows = snapshot.section("volume_total").sort_values("value")
    if rows.empty:
        return no_rows_figure(snapshot, "volume_total")
    fig = go.Figure(go.Bar(x=rows["value"], y=rows["dimension"], orientation="h",
                           marker_color=CATEGORICAL[0],
                           hovertemplate="%{y}: %{x:,.0f} records<extra></extra>"))
    return style_figure(fig, "Records stored per topic", "Records", "Silver topic", SOURCE)


def create_app(gold_folder: Path | None = None) -> Dash:
    app = Dash(__name__, title=TITLE, external_stylesheets=[dbc.themes.FLATLY])

    def page():
        # a function, so every page load (or browser reload) reads the newest Gold file
        snapshot = load_gold(KIND, gold_folder)
        return dbc.Container([
            header(TITLE, ANGLE, snapshot),
            dbc.Container([
                refresh_controls(),
                # Part 2 (governance): KPI cards, null rate per field, volume over time per source,
                # outlier rate per numeric field, quality summary table, data-health panel
                dbc.Row(dbc.Col(chart_card("records-per-topic"), lg=6)),
            ], fluid=True),
        ], fluid=True, className="px-0")

    app.layout = page

    @app.callback(Output("last-updated", "children"), Output("records-per-topic", "figure"),
                  Input("refresh-button", "n_clicks"), Input("refresh-interval", "n_intervals"))
    def refresh(_clicks, _ticks):
        current = load_gold(KIND, gold_folder)
        return last_updated_text(current), records_per_topic_figure(current)

    return app


def main() -> None:
    create_app().run(host=os.getenv("DASH_HOST", "127.0.0.1"),
                     port=int(os.getenv("GOVERNANCE_PORT", DEFAULT_PORT)), debug=False)


if __name__ == "__main__":
    main()
