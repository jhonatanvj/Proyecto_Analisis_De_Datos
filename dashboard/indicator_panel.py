"""Volcano Activity Indicator panel of the storytelling dashboard (Workshop 4 Part 1).

US1 map of every US volcano coloured by its official alert level, US6 VAI ranking filterable by
region, US3/US5/US7 weekly indicator history of one volcano with its eruptions, US8 which inputs the
indicator used. Reads only the vai_current, vai_weekly, volcano_map and eruption_history sections of
the storytelling Gold file. Kept in its own module so storytelling_app.py only adds one row and one
register_callbacks() call.
"""
from __future__ import annotations

from pathlib import Path

import dash_bootstrap_components as dbc
import pandas as pd
import plotly.graph_objects as go
from dash import Input, Output, State, ctx, dcc, html

from dashboard.common.gold import GoldSnapshot, load_gold
from dashboard.common.layout import chart_card, empty_figure, no_rows_figure
from dashboard.common.theme import ALERT_LEVEL_COLORS, VAI_LEVEL_COLORS, style_figure, style_map

ALL_REGIONS = "All regions"
SOURCE = "USGS HANS, USGS earthquakes, Smithsonian GVP via the Gold layer (storytelling_*.parquet)"
DISCLAIMER = ("The Volcano Activity Indicator (VAI, 0-100) compares each volcano with its own past. It is a "
              "monitoring aid, not an eruption forecast; official alert levels come from the USGS volcano "
              "observatories.")
ALERT_NAMES = {1: "NORMAL", 2: "ADVISORY", 3: "WATCH", 4: "WARNING"}
LEVEL_NAMES = {1: "LOW", 2: "MODERATE", 3: "ELEVATED", 4: "HIGH"}
COMPONENT_NAMES = {"reported": "official alert level", "seismic": "earthquakes vs. the volcano's past",
                   "eruptive": "eruptive history"}
TOP_N = 20


def _pivot(rows: pd.DataFrame) -> pd.DataFrame:
    return rows.pivot_table(index="dimension", columns="metric", values="value", aggfunc="first", dropna=False)


def indicator_table(snapshot: GoldSnapshot) -> pd.DataFrame:
    """One row per volcano: vnum, name, region, observatory, latitude, longitude, alert_level,
    vai_score, vai_level, components (list of plain names), components_used, earthquakes_window and
    the three component values."""
    columns = ["vnum", "name", "region", "observatory", "latitude", "longitude", "alert_level",
               "vai_score", "vai_level", "components", "components_used", "earthquakes_window",
               "reported", "seismic", "eruptive"]
    current_cols = ["vai_score", "level_ord", "components_used", "earthquakes_window", "reported", "seismic", "eruptive"]
    geo = snapshot.section("volcano_map")
    if geo.empty:
        return pd.DataFrame(columns=columns)
    table = _pivot(geo).reset_index()
    parts = table["dimension"].str.split("|", n=3, expand=True)
    table["vnum"], table["name"], table["region"], table["observatory"] = parts[0], parts[1], parts[2], parts[3]
    table["alert_level"] = table["alert_level_ord"].map(ALERT_NAMES).fillna("UNASSIGNED")

    vai = snapshot.section("vai_current")
    if vai.empty:
        current = pd.DataFrame(columns=["vnum"] + current_cols)
    else:
        current = _pivot(vai).reset_index().rename(columns={"dimension": "vnum"})
    for col in current_cols:
        if col not in current.columns:
            current[col] = float("nan")
    table = table.merge(current[["vnum"] + current_cols], on="vnum", how="left")
    table["vai_level"] = table["level_ord"].map(LEVEL_NAMES)
    table["components"] = [[COMPONENT_NAMES[c] for c in COMPONENT_NAMES if pd.notna(r[c])]
                           for _, r in table.iterrows()]
    return table[columns]


def map_figure(table: pd.DataFrame) -> go.Figure:
    """Every volcano on a map, one colour per official alert level, bigger markers for a higher VAI."""
    if table.empty:
        return empty_figure("No indicator data in this Gold file yet: run the gold_processing DAG")
    fig = go.Figure()
    for alert in ["WARNING", "WATCH", "ADVISORY", "NORMAL", "UNASSIGNED"]:
        rows = table[table["alert_level"] == alert]
        if rows.empty:
            continue
        vai = rows["vai_score"].fillna(0)
        vai_text = rows["vai_score"].map(lambda v: "no indicator" if pd.isna(v) else f"{v:.1f}")
        fig.add_trace(go.Scattermap(
            lat=rows["latitude"], lon=rows["longitude"], mode="markers", name=alert,
            marker={"size": 7 + vai / 6, "color": ALERT_LEVEL_COLORS[alert], "opacity": 0.85},
            customdata=rows[["vnum", "name", "region"]].assign(vai=vai_text),
            hovertemplate="<b>%{customdata[1]}</b> (%{customdata[2]})<br>Alert level: " + alert
                          + "<br>VAI: %{customdata[3]}<extra></extra>"))
    fig.update_layout(map={"style": "carto-positron", "center": {"lat": 48, "lon": -165}, "zoom": 1.6},
                      legend={"title": {"text": "USGS alert level"}, "orientation": "v", "x": 0.99, "xanchor": "right",
                              "y": 0.98, "yanchor": "top", "bgcolor": "rgba(255,255,255,0.85)"},
                      height=460)
    return style_map(fig, "Official alert level of every US volcano (marker size = indicator)", SOURCE)


def ranking_figure(table: pd.DataFrame, region: str) -> go.Figure:
    """Top volcanoes by VAI in the region, highest at the top, coloured by VAI level."""
    rows = table.dropna(subset=["vai_score"])
    if region != ALL_REGIONS:
        rows = rows[rows["region"] == region]
    if rows.empty:
        return empty_figure("No indicator values for this region")
    rows = rows.sort_values("vai_score", ascending=False).head(TOP_N).iloc[::-1]
    fig = go.Figure()
    # one trace per level, so the legend explains the colours; a score built on a single input (for
    # example one or two earthquakes at an unmonitored volcano) is drawn lighter, because the
    # renormalisation gives that input the whole weight
    for lvl in ["HIGH", "ELEVATED", "MODERATE", "LOW"]:
        part = rows[rows["vai_level"] == lvl]
        if part.empty:
            continue
        inputs = part["components_used"].fillna(0).astype(int)
        fig.add_trace(go.Bar(
            x=part["vai_score"], y=part["name"], orientation="h", name=lvl,
            marker={"color": VAI_LEVEL_COLORS[lvl], "opacity": [1.0 if n >= 2 else 0.45 for n in inputs]},
            customdata=part[["vnum", "vai_level", "region"]].assign(
                inputs=inputs, quakes=part["earthquakes_window"].fillna(0).astype(int)),
            hovertemplate="<b>%{y}</b> (%{customdata[2]})<br>VAI %{x:.1f}: %{customdata[1]}"
                          "<br>Based on %{customdata[3]} of 3 inputs; %{customdata[4]} earthquakes in the last 7 days"
                          "<extra></extra>"))
    fig.update_xaxes(range=[0, 100])
    # every name on the axis (Plotly skips labels on a crowded category axis), highest at the top
    fig.update_yaxes(categoryorder="array", categoryarray=list(rows["name"]), dtick=1, tickfont={"size": 11})
    fig.update_layout(height=max(300, 22 * len(rows) + 160), barmode="overlay")
    title = ("Most active volcanoes compared with their own past" + ("" if region == ALL_REGIONS else f": {region}")
             + "<br><sup>Lighter bars: the score rests on one input only</sup>")
    return style_figure(fig, title, "Volcano Activity Indicator (0-100)", "", SOURCE)


def history_figure(snapshot: GoldSnapshot, vnum: str | None) -> go.Figure:
    """Weekly VAI (earthquakes and eruptive history) of one volcano, with its eruptions marked."""
    weekly = snapshot.section("vai_weekly")
    if weekly.empty:
        return no_rows_figure(snapshot, "vai_weekly")
    rows = weekly[(weekly["dimension"] == vnum) & (weekly["metric"] == "vai_score")].sort_values("period")
    names = indicator_table(snapshot).set_index("vnum")["name"]
    name = names.get(vnum, vnum)
    if rows.empty:
        return empty_figure(f"No weekly history for {name}: it needs at least 20 earthquakes since 2010")
    weeks = pd.to_datetime(rows["period"])
    # a single earthquake in a usually silent week already looks unusual, so the weekly value jumps;
    # the 4-week average is the line to read, the weekly values stay in the background
    fig = go.Figure(go.Scatter(x=weeks, y=rows["value"], mode="lines", name="Weekly value",
                               line={"color": "#56B4E9", "width": 1}, opacity=0.45,
                               hovertemplate="Week of %{x|%b %d, %Y}: %{y:.0f}<extra></extra>"))
    fig.add_trace(go.Scatter(x=weeks, y=rows["value"].rolling(4, min_periods=1).mean(), mode="lines",
                             name="4-week average", line={"color": "#0072B2", "width": 2.5},
                             hovertemplate="4 weeks to %{x|%b %d, %Y}: %{y:.0f}<extra></extra>"))
    eruptions = snapshot.section("eruption_history")
    eruptions = eruptions[(eruptions["dimension"] == vnum) & (eruptions["metric"] == "vei")]
    eruptions = eruptions[pd.to_datetime(eruptions["period"]) >= pd.to_datetime(rows["period"]).min()]
    fig.add_trace(go.Scatter(
        x=pd.to_datetime(eruptions["period"]), y=[97] * len(eruptions), mode="markers", name="Eruption start",
        marker={"symbol": "triangle-down", "size": 12, "color": "#D55E00"},
        customdata=eruptions["value"].map(lambda v: "unknown" if pd.isna(v) else f"{v:.0f}"),
        hovertemplate="Eruption started %{x|%b %d, %Y}<br>VEI %{customdata}<extra></extra>"))
    for bound, label in [(50, "ELEVATED"), (75, "HIGH")]:
        fig.add_hline(y=bound, line={"color": "#999999", "dash": "dot"}, annotation_text=label,
                      annotation_position="top left")
    fig.update_yaxes(range=[0, 100])
    fig.update_layout(height=380)
    return style_figure(fig, f"{name}: indicator week by week since 2010 (earthquakes and eruptions only)",
                        "Week", "Volcano Activity Indicator (0-100)", SOURCE)


def components_text(table: pd.DataFrame, vnum: str | None) -> str:
    """Plain-language note on which inputs the current VAI of a volcano used (US8)."""
    row = table[table["vnum"] == vnum]
    if row.empty or pd.isna(row["vai_score"].iloc[0]):
        return "Choose a volcano to see which inputs its indicator uses."
    r = row.iloc[0]
    missing = [COMPONENT_NAMES[c] for c in COMPONENT_NAMES if COMPONENT_NAMES[c] not in r["components"]]
    used = ", ".join(f"{COMPONENT_NAMES[c]} ({r[c]:.2f})" for c in COMPONENT_NAMES if pd.notna(r[c]))
    text = f"{r['name']}: indicator {r['vai_score']:.1f} ({r['vai_level']}). Based on {used}."
    if missing:
        text += " Not available: " + ", ".join(missing) + "."
    return text + (" The weekly history uses earthquakes and eruptions only, because past alert levels "
                   "are not kept by USGS HANS.")


def pick_volcano(table: pd.DataFrame, region: str, current: str | None, clicked: str | None,
                 region_changed: bool = False) -> str | None:
    """Volcano to show in the history.

    A click on a volcano with an indicator wins. Otherwise the current choice stays, even outside the
    region (a refresh must not undo a click on the map); only a change of region moves it to the top
    volcano of the new region. Volcanoes without an indicator cannot be chosen.
    """
    rated = table.dropna(subset=["vai_score"])
    in_region = rated if region == ALL_REGIONS else rated[rated["region"] == region]
    if clicked and clicked in set(rated["vnum"]):
        return clicked
    allowed = set(in_region["vnum"]) if region_changed else set(rated["vnum"])
    if current in allowed:
        return current
    return None if in_region.empty else in_region.sort_values("vai_score", ascending=False)["vnum"].iloc[0]


def _regions(table: pd.DataFrame) -> list[str]:
    return [ALL_REGIONS] + sorted(table["region"].dropna().unique())


def layout() -> dbc.Container:
    return dbc.Container([
        html.H2("Volcano Activity Indicator", className="h4 mt-2"),
        dbc.Alert(DISCLAIMER, color="light", className="small border"),
        dbc.Row([
            dbc.Col(chart_card("volcano-map"), lg=7),
            dbc.Col([
                dcc.Dropdown(id="vai-region", value=ALL_REGIONS, clearable=False, className="mb-2"),
                chart_card("vai-ranking"),
            ], lg=5),
        ]),
        dbc.Row([
            dbc.Col([
                dcc.Dropdown(id="vai-volcano", placeholder="Choose a volcano (or click one on the map)",
                             className="mb-2"),
                chart_card("volcano-history"),
            ], lg=8),
            dbc.Col(dbc.Card(dbc.CardBody([html.H3("What the indicator uses", className="h6"),
                                           html.P(id="vai-components", className="small mb-0")]),
                             className="shadow-sm"), lg=4),
        ]),
    ], fluid=True, className="px-0")


def _clicked(click) -> str | None:
    points = (click or {}).get("points") or []
    return points[0]["customdata"][0] if points and points[0].get("customdata") else None


def register_callbacks(app, gold_folder: Path | None = None) -> None:
    @app.callback(Output("volcano-map", "figure"), Output("vai-ranking", "figure"),
                  Output("vai-region", "options"), Output("vai-volcano", "options"), Output("vai-volcano", "value"),
                  Input("refresh-button", "n_clicks"), Input("refresh-interval", "n_intervals"),
                  Input("vai-region", "value"), Input("volcano-map", "clickData"), Input("vai-ranking", "clickData"),
                  State("vai-volcano", "value"))
    def update_overview(_clicks, _ticks, region, map_click, ranking_click, current):
        table = indicator_table(load_gold("storytelling", gold_folder))
        region = region or ALL_REGIONS
        clicked = {"volcano-map": _clicked(map_click), "vai-ranking": _clicked(ranking_click)}.get(ctx.triggered_id)
        rated = table.dropna(subset=["vai_score"]).sort_values("name")
        options = [{"label": f"{r.name} ({r.region})", "value": r.vnum} for r in rated.itertuples()]
        return (map_figure(table), ranking_figure(table, region), _regions(table), options,
                pick_volcano(table, region, current, clicked, region_changed=ctx.triggered_id == "vai-region"))

    @app.callback(Output("volcano-history", "figure"), Output("vai-components", "children"),
                  Input("vai-volcano", "value"), Input("refresh-interval", "n_intervals"))
    def update_volcano(vnum, _ticks):
        snapshot = load_gold("storytelling", gold_folder)
        return history_figure(snapshot, vnum), components_text(indicator_table(snapshot), vnum)
