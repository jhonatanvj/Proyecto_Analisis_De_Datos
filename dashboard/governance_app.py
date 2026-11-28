"""Governance dashboard (Workshop 4), http://localhost:8050

Data quality of the volcano pipeline for the data team, read only from the latest
datalake_gold/governance_*.parquet (sections written by src/volcano_pipeline/gold/governance.py).

Layout, top to bottom:
* filters: source and Silver topic (they narrow the charts and tables that have that breakdown);
* KPI cards: headline value of six indicators with a pass / warning badge (section kpi_summary);
* null rate per field (null_rate) | outlier rate per numeric field (outlier_rate);
* records per week and source (volume_weekly) | records stored per topic (volume_total);
* quality summary table: every KPI with value, threshold and status (kpi_summary);
* data health per source, user story US4: records, rejected records, newest file, file and
  watermark age (data_health, freshness).

Every number comes from Gold; the app only filters, sorts and formats. Thresholds live in
config/sources.yaml (governance.thresholds) and reach the app through the kpi_summary rows.

Run: python -m dashboard.governance_app   (or the governance-dashboard service in docker compose)
"""
from __future__ import annotations

import os
from pathlib import Path

import dash_bootstrap_components as dbc
import pandas as pd
import plotly.graph_objects as go
from dash import Dash, Input, Output, dcc, html

from dashboard.common.gold import GoldSnapshot, load_gold
from dashboard.common.layout import chart_card, header, last_updated_text, no_rows_figure, refresh_controls
from dashboard.common.theme import CATEGORICAL, style_figure

KIND = "governance"
TITLE = "Data Governance Dashboard"
ANGLE = "Data quality of the volcano pipeline, for the data team"
SOURCE = "USGS / GVP via the Gold layer (governance_*.parquet)"
DEFAULT_PORT = 8050

PASS_COLOR, WARN_COLOR = "#009E73", "#D55E00"      # Okabe-Ito green / vermillion, with a text label
TOP_FIELDS = 20                                      # null-rate bars: the 20 worst fields

# KPI name -> (label shown to the data team, what the headline value means)
KPI_LABELS = {
    "volume": ("Total records", "records stored in Silver"),
    "null_rate": ("Null rate", "mean % of empty values per field"),
    "duplicate_rate": ("Duplicate rate", "% exact copies in Silver"),
    "schema_compliance": ("Schema compliance", "lowest % of files matching the contract, per topic"),
    "quarantine_rate": ("Quarantine rate", "% of processed records rejected"),
    "outlier_rate": ("Outlier rate", "highest % of outliers of a numeric field"),
    "text_length": ("Encoding errors", "% of notices with garbled text (text_length KPI)"),
    "freshness": ("Freshness", "% of sources with a file younger than a day (2 x schedule if longer)"),
    "ingestion_frequency": ("Ingestion frequency", "% of the last 7 days' hours with a new usgs_feed file"),
}
CARD_KPIS = ["volume", "null_rate", "duplicate_rate", "schema_compliance", "quarantine_rate", "freshness"]
FIELD_LABELS = {"earthquakes.mag": "Magnitude", "earthquakes.depth_km": "Depth (km)"}
SCHEDULE_MAX_AGE = "a day (twice the schedule interval if longer)"


# ------------------------------------------------------------------------------- helpers

def fmt(value: float | None, unit: str = "") -> str:
    """Readable number: thousands separators, few decimals, small percentages kept visible."""
    if value is None or pd.isna(value):
        return "n/a"
    unit = unit.split("|")[0].strip()
    if unit == "%":
        return f"{value:.3g} %" if 0 < abs(value) < 1 else f"{value:,.1f} %"
    if unit == "records":
        return f"{value:,.0f}"
    if unit == "hours":
        return f"{value:,.1f} h"
    return f"{value:,.2f}"


def topic_of(field: str) -> str:
    return field.split(".", 1)[0]


def kpi_table(snapshot: GoldSnapshot) -> pd.DataFrame:
    """kpi_summary pivoted to one row per KPI: value, unit, threshold, rule, status."""
    rows = snapshot.section("kpi_summary")
    columns = ["kpi", "metric", "value", "unit", "threshold", "rule", "status"]
    if rows.empty:
        return pd.DataFrame(columns=columns)
    values = rows[~rows["metric"].str.startswith("threshold_") & (rows["metric"] != "status")]
    out = values.rename(columns={"dimension": "kpi"})[["kpi", "metric", "value", "unit"]].copy()
    limits = rows[rows["metric"].str.startswith("threshold_")]
    out["threshold"] = out["kpi"].map(dict(zip(limits["dimension"], limits["value"])))
    out["rule"] = out["kpi"].map(dict(zip(limits["dimension"], limits["metric"].str.removeprefix("threshold_"))))
    status = rows[rows["metric"] == "status"]
    out["status"] = out["kpi"].map(dict(zip(status["dimension"], status["value"])))
    order = {k: i for i, k in enumerate(KPI_LABELS)}
    return out.sort_values("kpi", key=lambda s: s.map(order)).reset_index(drop=True)[columns]


def status_badge(status: float | None) -> dbc.Badge:
    if status is None or pd.isna(status):
        return dbc.Badge("Info", color="secondary", className="ms-1")
    ok = status >= 1
    return dbc.Badge("Pass" if ok else "Warning", color="success" if ok else "danger", className="ms-1",
                     style={"backgroundColor": PASS_COLOR if ok else WARN_COLOR})


def threshold_text(row) -> str:
    if pd.isna(row["threshold"]):
        return "-"
    return f"{'≤' if row['rule'] == 'max' else '≥'} {fmt(row['threshold'], row['unit'])}"


# ---------------------------------------------------------------------------- components

def kpi_cards(snapshot: GoldSnapshot) -> list:
    """One card per headline KPI: label, value, pass / warning badge and the threshold."""
    table = kpi_table(snapshot).set_index("kpi")
    cards = []
    for kpi in CARD_KPIS:
        label, meaning = KPI_LABELS[kpi]
        if kpi in table.index:
            row = table.loc[kpi]
            value, status, limit = fmt(row["value"], row["unit"]), row["status"], threshold_text(row)
        else:
            value, status, limit = "n/a", None, "-"
        border = "secondary" if status is None or pd.isna(status) else ("success" if status >= 1 else "danger")
        cards.append(dbc.Col(dbc.Card(dbc.CardBody([
            html.Div([html.Span(label, className="small fw-semibold text-muted"), status_badge(status)]),
            html.Div(value, className="h4 mb-0 mt-1"),
            html.Div(meaning, className="small text-muted"),
            html.Div(f"Threshold: {limit}", className="small"),
        ]), color=border, outline=True, className="h-100 shadow-sm"), xs=12, sm=6, lg=2, className="mb-3"))
    return cards


def null_rate_figure(snapshot: GoldSnapshot, topics: list[str] | None = None) -> go.Figure:
    """Fields with empty values, worst first (only fields above 0 %, at most TOP_FIELDS)."""
    rows = snapshot.section("null_rate")
    if rows.empty:
        return no_rows_figure(snapshot, "null_rate")
    if topics:
        rows = rows[rows["dimension"].map(topic_of).isin(topics)]
    rows = rows[rows["value"] > 0].nlargest(TOP_FIELDS, "value").sort_values("value")
    if rows.empty:
        fig = go.Figure()
        fig.add_annotation(text="No empty values in the selected topics", x=0.5, y=0.5, xref="paper",
                           yref="paper", showarrow=False)
        return style_figure(fig, "Share of empty values per field", "Empty values (%)", "Field", SOURCE)
    fig = go.Figure(go.Bar(x=rows["value"], y=rows["dimension"], orientation="h", marker_color=CATEGORICAL[0],
                           hovertemplate="%{y}: %{x:.2f} % empty<extra></extra>"))
    fig.update_layout(height=max(380, 28 * len(rows) + 160))
    return style_figure(fig, f"Share of empty values per field (worst {len(rows)})", "Empty values (%)",
                        "Field (topic.column)", SOURCE)


def outlier_figure(snapshot: GoldSnapshot) -> go.Figure:
    """Outlier rate per numeric field: Gold (per-volcano fences) next to Silver's per-file flags."""
    rows = snapshot.section("outlier_rate")
    if rows.empty:
        return no_rows_figure(snapshot, "outlier_rate")
    names = {"pct_outlier": "Per-volcano fences (KPI)", "pct_outlier_silver_flags": "Silver flags, per file"}
    fig = go.Figure()
    for i, (metric, label) in enumerate(names.items()):
        part = rows[rows["metric"] == metric]
        fig.add_trace(go.Bar(x=part["dimension"].map(lambda d: FIELD_LABELS.get(d, d)), y=part["value"], name=label,
                             marker_color=CATEGORICAL[i], hovertemplate="%{x}: %{y:.2f} %<extra>" + label + "</extra>"))
    limit = kpi_table(snapshot).set_index("kpi")["threshold"].get("outlier_rate")
    if limit is not None and not pd.isna(limit):
        fig.add_hline(y=limit, line_dash="dash", line_color="#666666",
                      annotation_text=f"threshold {limit:g} %", annotation_position="top left")
    fig.update_layout(barmode="group", height=420)
    return style_figure(fig, "Outlier rate per numeric field (earthquakes)", "Field", "Outliers (% of records)",
                        SOURCE)


def volume_figure(snapshot: GoldSnapshot, sources: list[str] | None = None) -> go.Figure:
    """Records per week and source (event week); falls back to per-topic rows in older Gold files."""
    rows = snapshot.section("volume_weekly")
    if rows.empty:
        return no_rows_figure(snapshot, "volume_weekly")
    by_source = rows[rows["metric"] == "records_by_source"]
    rows, legend = (by_source, "Source") if not by_source.empty else (rows[rows["metric"] == "records"], "Topic")
    if sources and legend == "Source":
        rows = rows[rows["dimension"].isin(sources)]
    fig = go.Figure()
    for i, (name, part) in enumerate(sorted(rows.groupby("dimension"), key=lambda kv: -kv[1]["value"].sum())):
        part = part.sort_values("period")
        fig.add_trace(go.Scatter(x=pd.to_datetime(part["period"]), y=part["value"], name=name, mode="lines",
                                 line={"width": 1.4, "color": CATEGORICAL[i % len(CATEGORICAL)]},
                                 hovertemplate="week of %{x|%Y-%m-%d}: %{y:,.0f} records<extra>" + name
                                 + "</extra>"))
    # log axis with ticks at powers of ten only: earthquakes (~1,000/week) and notices (~20/week) differ
    # by two orders of magnitude; zoom on a period with the toolbar or by dragging
    fig.update_layout(height=420, legend_title_text=legend)
    fig.update_yaxes(type="log", dtick=1)
    return style_figure(fig, f"Records per week and {legend.lower()} (event time)", "Week",
                        "Records per week (log scale)", SOURCE)


def records_per_topic_figure(snapshot: GoldSnapshot) -> go.Figure:
    """Records stored per Silver topic (section volume_total), including the quarantine."""
    rows = snapshot.section("volume_total").sort_values("value")
    if rows.empty:
        return no_rows_figure(snapshot, "volume_total")
    fig = go.Figure(go.Bar(x=rows["value"], y=rows["dimension"], orientation="h",
                           marker_color=CATEGORICAL[0],
                           hovertemplate="%{y}: %{x:,.0f} records<extra></extra>"))
    fig.update_layout(height=420, xaxis_type="log")
    return style_figure(fig, "Records stored per topic", "Records (log scale)", "Silver topic", SOURCE)


def quality_table(snapshot: GoldSnapshot) -> dbc.Table | html.Div:
    """Every KPI with its headline value, threshold and status."""
    table = kpi_table(snapshot)
    if table.empty:
        return html.Div("No kpi_summary rows in this Gold file yet: run the gold_processing DAG.",
                        className="text-muted")
    body = [html.Tr([
        html.Td(KPI_LABELS.get(r["kpi"], (r["kpi"], ""))[0], className="fw-semibold"),
        html.Td(KPI_LABELS.get(r["kpi"], ("", r["metric"]))[1], className="small text-muted"),
        html.Td(fmt(r["value"], r["unit"]), className="text-end"),
        html.Td(threshold_text(r), className="text-end"),
        html.Td(status_badge(r["status"])),
    ]) for _, r in table.iterrows()]
    head = html.Thead(html.Tr([html.Th(h) for h in ["KPI", "Measures", "Value", "Threshold", "Status"]]))
    return dbc.Table([head, html.Tbody(body)], id="quality-table", hover=True, size="sm", responsive=True)


def health_frame(snapshot: GoldSnapshot, sources: list[str] | None = None) -> pd.DataFrame:
    """data_health and freshness pivoted to one row per source."""
    rows = pd.concat([snapshot.section("data_health"), snapshot.section("freshness")])
    if rows.empty:
        return pd.DataFrame()
    wide = rows.pivot_table(index="dimension", columns="metric", values="value", aggfunc="first")
    if sources:
        wide = wide[wide.index.isin(sources)]
    return wide.sort_index()


def health_table(snapshot: GoldSnapshot, sources: list[str] | None = None) -> dbc.Table | html.Div:
    """Data health per source (US4): last successful file, its age, watermark age, records, rejects."""
    wide = health_frame(snapshot, sources)
    if wide.empty:
        return html.Div("No data_health rows in this Gold file yet: run the gold_processing DAG.",
                        className="text-muted")

    def cell(source, metric, unit):
        return fmt(wide.at[source, metric], unit) if metric in wide.columns else "n/a"

    body = []
    for source in wide.index:
        last = wide.at[source, "last_file_utc"] if "last_file_utc" in wide.columns else None
        when = "n/a" if last is None or pd.isna(last) else f"{pd.to_datetime(last, unit='s'):%Y-%m-%d %H:%M}"
        body.append(html.Tr([
            html.Td(source, className="fw-semibold"), html.Td(when),
            html.Td(cell(source, "hours_since_last", "hours"), className="text-end"),
            html.Td(cell(source, "watermark_age_hours", "hours"), className="text-end"),
            html.Td(cell(source, "records_in_silver", "records"), className="text-end"),
            html.Td(cell(source, "rejected_records", "records") if "rejected_records" in wide.columns
                    and not pd.isna(wide.at[source, "rejected_records"]) else "0", className="text-end"),
        ]))
    head = html.Thead(html.Tr([html.Th(h) for h in ["Source", "Newest file (UTC)", "File age", "Watermark age",
                                                     "Records in Silver", "Rejected"]]))
    note = html.Div("File age: time since the newest Bronze file that reached Silver. Watermark age: time since "
                    "the newest source timestamp stored (GVP has a database version, not a timestamp). "
                    f"A source is fresh when its file age is below {SCHEDULE_MAX_AGE} (Freshness card).",
                    className="small text-muted")
    return html.Div([dbc.Table([head, html.Tbody(body)], id="health-table", hover=True, size="sm",
                               responsive=True), note])


def section_card(title: str, body_id: str) -> dbc.Card:
    return dbc.Card([dbc.CardHeader(title, className="fw-semibold"), dbc.CardBody(html.Div(id=body_id))],
                    className="mb-3 shadow-sm")


def filter_options(snapshot: GoldSnapshot) -> tuple[list[str], list[str]]:
    sources = sorted(set(snapshot.section("data_health")["dimension"]) |
                     set(snapshot.section("volume_weekly").query("metric == 'records_by_source'")["dimension"]))
    topics = sorted(set(snapshot.section("null_rate")["dimension"].map(topic_of)))
    return sources, topics


# ----------------------------------------------------------------------------------- app

def create_app(gold_folder: Path | None = None) -> Dash:
    app = Dash(__name__, title=TITLE, external_stylesheets=[dbc.themes.FLATLY])

    def page():
        # a function, so every page load (or browser reload) reads the newest Gold file
        snapshot = load_gold(KIND, gold_folder)
        sources, topics = filter_options(snapshot)
        filters = dbc.Row([
            dbc.Col([html.Label("Source", className="small fw-semibold"),
                     dcc.Dropdown(sources, multi=True, id="source-filter", placeholder="All sources")], md=4),
            dbc.Col([html.Label("Silver topic", className="small fw-semibold"),
                     dcc.Dropdown(topics, multi=True, id="topic-filter", placeholder="All topics")], md=4),
            dbc.Col(html.Div("Source narrows the volume chart and the data-health table; topic narrows the "
                             "null-rate chart.", className="small text-muted"), md=4, className="align-self-end"),
        ], className="mb-3")
        return dbc.Container([
            header(TITLE, ANGLE, snapshot),
            dbc.Container([
                refresh_controls(),
                filters,
                dbc.Row(id="kpi-cards"),
                dbc.Row([dbc.Col(chart_card("null-rate"), lg=6), dbc.Col(chart_card("outlier-rate"), lg=6)]),
                dbc.Row([dbc.Col(chart_card("volume-over-time"), lg=8),
                         dbc.Col(chart_card("records-per-topic"), lg=4)]),
                dbc.Row([dbc.Col(section_card("Quality summary: every KPI against its threshold", "quality-summary"),
                                 lg=6),
                         dbc.Col(section_card("Data health per source (US4)", "data-health"), lg=6)]),
            ], fluid=True),
        ], fluid=True, className="px-0")

    app.layout = page

    @app.callback(Output("last-updated", "children"), Output("kpi-cards", "children"),
                  Output("null-rate", "figure"), Output("outlier-rate", "figure"),
                  Output("volume-over-time", "figure"), Output("records-per-topic", "figure"),
                  Output("quality-summary", "children"), Output("data-health", "children"),
                  Input("refresh-button", "n_clicks"), Input("refresh-interval", "n_intervals"),
                  Input("source-filter", "value"), Input("topic-filter", "value"))
    def refresh(_clicks, _ticks, sources, topics):
        current = load_gold(KIND, gold_folder)
        return (last_updated_text(current), kpi_cards(current), null_rate_figure(current, topics),
                outlier_figure(current), volume_figure(current, sources), records_per_topic_figure(current),
                quality_table(current), health_table(current, sources))

    return app


def main() -> None:
    create_app().run(host=os.getenv("DASH_HOST", "127.0.0.1"),
                     port=int(os.getenv("GOVERNANCE_PORT", DEFAULT_PORT)), debug=False)


if __name__ == "__main__":
    main()
