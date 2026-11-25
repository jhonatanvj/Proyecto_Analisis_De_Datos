"""Layout pieces both dashboards share: header, refresh controls, chart cards, empty figures."""
from __future__ import annotations

import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import dcc, html

from .gold import GoldSnapshot
from .theme import TEMPLATE

PROJECT_TITLE = "Volcanic Activity Monitoring: United States"
TEAM = "Team 6 · Data Analysis Programming 2026-II · Universidad Distrital"


def last_updated_text(snapshot: GoldSnapshot) -> str:
    if snapshot.generated_utc is None:
        return "Data last updated: no Gold data yet"
    return f"Data last updated: {snapshot.generated_utc:%Y-%m-%d %H:%M} UTC"


def header(app_title: str, angle: str, snapshot: GoldSnapshot) -> dbc.Container:
    """Project title, dashboard name, topic angle, team and the Gold timestamp."""
    return dbc.Container(
        dbc.Row([
            dbc.Col([
                html.Div(PROJECT_TITLE, className="text-uppercase small fw-semibold text-white-50"),
                html.H1(app_title, className="h3 mb-1 text-white"),
                html.Div(angle, className="text-white"),
                html.Div(TEAM, className="small text-white-50"),
            ], md=8),
            dbc.Col(html.Div(last_updated_text(snapshot), id="last-updated",
                             className="small text-white text-md-end"),
                    md=4, className="align-self-end"),
        ]),
        fluid=True, className="bg-primary py-3 px-4 mb-3",
    )


def refresh_controls(minutes: int = 5) -> html.Div:
    """A manual refresh button plus an interval that checks for a newer Gold file."""
    return html.Div([
        dbc.Button("Refresh data", id="refresh-button", color="secondary", size="sm", outline=True),
        html.Span(f"The page also checks for new data every {minutes} minutes.",
                  className="small text-muted ms-2"),
        dcc.Interval(id="refresh-interval", interval=minutes * 60_000),
    ], className="mb-3")


def chart_card(graph_id: str) -> dbc.Card:
    """One chart in a card; the figure itself (title, axes, source) comes from a callback."""
    return dbc.Card(dbc.CardBody(dcc.Graph(id=graph_id, config={"displaylogo": False})),
                    className="mb-3 shadow-sm")


def no_rows_figure(snapshot: GoldSnapshot, section: str) -> go.Figure:
    """Placeholder for a chart whose section has no rows: says whether Gold is missing or only that section."""
    if snapshot.path is None:
        return empty_figure("No Gold data yet: run the gold_processing DAG")
    return empty_figure(f"This Gold file has no {section} rows yet")


def empty_figure(message: str) -> go.Figure:
    """Placeholder figure when Gold has no rows for a chart yet."""
    fig = go.Figure()
    fig.update_layout(template=TEMPLATE, xaxis={"visible": False}, yaxis={"visible": False})
    fig.add_annotation(text=message, xref="paper", yref="paper", x=0.5, y=0.5,
                       showarrow=False, font={"size": 14, "color": "#666666"})
    return fig
