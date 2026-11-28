"""Storytelling dashboard (Workshop 4), http://localhost:8051

Which US volcanoes show unusual activity, and is it building or fading? Written for the
emergency-management analyst of Workshop 1, read only from the latest
datalake_gold/storytelling_*.parquet.

Run: python -m dashboard.storytelling_app
"""
from __future__ import annotations

import os
from pathlib import Path

import dash_bootstrap_components as dbc
import pandas as pd
import plotly.graph_objects as go
from dash import Dash, Input, Output, dcc, html
from plotly.subplots import make_subplots

from dashboard import indicator_panel
from dashboard.common.gold import GoldSnapshot, load_gold
from dashboard.common.layout import chart_card, header, last_updated_text, no_rows_figure, refresh_controls
from dashboard.common.theme import CATEGORICAL, TEMPLATE, TONE_COLORS, style_figure

KIND = "storytelling"
TITLE = "Volcano Activity Story"
ANGLE = "Which US volcanoes show unusual activity, and is it building or fading?"
SOURCE = "USGS HANS notices, Volcano Updates & FDSN via Gold (storytelling_*.parquet)"
DEFAULT_PORT = 8051

SOURCE_LABELS = {
    "usgs_hans_notices_api": "HANS API",
    "usgs_volcano_updates_scraping": "Volcano Updates",
    "usgs_earthquakes": "USGS Earthquakes (FDSN / Feed)",
}


def build_narrative_card(snapshot: GoldSnapshot) -> dbc.Card:
    """Plain-language executive insight card generated dynamically from Gold rows."""
    hl = snapshot.section("narrative_highlights")
    dist = snapshot.section("tone_distribution")
    alerts = snapshot.section("alerts_breakdown")

    if hl.empty and dist.empty and alerts.empty:
        body = [
            html.H5("No Gold storytelling summary available yet", className="card-title mb-2"),
            html.P(
                "Trigger the gold_processing DAG in Airflow (localhost:8080) and click Refresh data "
                "to populate the volcanic activity narrative.",
                className="mb-0 text-muted",
            ),
        ]
        return dbc.Card(dbc.CardBody(body), className="shadow-sm border-start border-4 border-secondary mb-3")

    elevated_count = None
    if not hl.empty and not hl[hl["metric"] == "elevated_volcanoes_count"].empty:
        elevated_count = int(round(hl[hl["metric"] == "elevated_volcanoes_count"]["value"].iloc[0]))
    elif not alerts.empty:
        obs_rows = alerts[alerts["metric"] == "volcanoes_by_observatory_alert"].copy()
        if not obs_rows.empty:
            obs_rows["level"] = obs_rows["dimension"].str.split("|").str[-1]
            elevated_count = int(obs_rows[obs_rows["level"].isin(["ADVISORY", "WATCH", "WARNING"])]["value"].sum())

    latest_tone = None
    wow_delta = None
    top_volcano = None
    top_score = None
    if not hl.empty:
        r_lt = hl[hl["metric"] == "latest_week_tone"]
        if not r_lt.empty:
            latest_tone = float(r_lt["value"].iloc[0])
        r_dl = hl[hl["metric"] == "tone_wow_delta"]
        if not r_dl.empty:
            wow_delta = float(r_dl["value"].iloc[0])
        r_tv = hl[hl["metric"] == "top_escalating_volcano"]
        if not r_tv.empty:
            top_volcano = str(r_tv["dimension"].iloc[0])
            top_score = float(r_tv["value"].iloc[0])

    esc_pct = None
    if not dist.empty and not dist[(dist["metric"] == "pct_records") & (dist["dimension"] == "escalating")].empty:
        esc_pct = float(dist[(dist["metric"] == "pct_records") & (dist["dimension"] == "escalating")]["value"].iloc[0])

    if wow_delta is not None:
        if wow_delta > 0.03:
            trend_phrase = f"escalated by +{wow_delta:.2f} points compared with the prior week"
            badge_color = "danger"
            badge_text = "Unrest Building"
        elif wow_delta < -0.03:
            trend_phrase = f"eased by {wow_delta:.2f} points compared with the prior week"
            badge_color = "success"
            badge_text = "Unrest Fading"
        else:
            trend_phrase = f"remained broadly steady ({wow_delta:+.2f} vs. prior week)"
            badge_color = "secondary"
            badge_text = "Steady Posture"
    else:
        trend_phrase = "reflects the latest consolidated USGS observatory bulletins"
        badge_color = "info"
        badge_text = "Network Overview"

    bullets = []
    if elevated_count is not None:
        bullets.append(
            html.Li([
                html.Strong("Current Alert Posture: "),
                f"{elevated_count} U.S. volcano(es) currently carry an elevated official USGS alert "
                "(Advisory, Watch, or Warning).",
            ])
        )
    if latest_tone is not None:
        bullets.append(
            html.Li([
                html.Strong("Weekly Bulletin Trajectory: "),
                f"Network-wide notice activity tone stands at {latest_tone:+.2f} (-1 calming to +1 escalating) "
                f"and has {trend_phrase}.",
            ])
        )
    if top_volcano is not None and top_score is not None:
        bullets.append(
            html.Li([
                html.Strong("Primary Focus Volcano: "),
                f"{top_volcano} recorded the highest escalation language in the latest reporting week "
                f"(activity tone {top_score:+.2f}).",
            ])
        )
    if esc_pct is not None:
        bullets.append(
            html.Li([
                html.Strong("Historical Archive Context: "),
                f"{esc_pct:.1f}% of all preprocessed USGS HANS notices since 2010 express escalating volcanic activity.",
            ])
        )

    return dbc.Card(
        dbc.CardBody([
            html.Div([
                html.H5("Operational Narrative Summary", className="card-title mb-0 me-2"),
                dbc.Badge(badge_text, color=badge_color, className="align-self-center"),
            ], className="d-flex align-items-center mb-2"),
            html.P(
                "Key takeaways synthesized from official USGS observatory notices, scraped updates, "
                "and seismic monitoring networks:",
                className="text-muted small mb-2",
            ),
            html.Ul(bullets, className="mb-0"),
        ]),
        className="shadow-sm border-start border-4 border-primary mb-3",
    )


def tone_donut_figure(snapshot: GoldSnapshot) -> go.Figure:
    """Donut chart: share of notices classified as calming, stable, or escalating."""
    dist = snapshot.section("tone_distribution")
    rows = dist[dist["metric"] == "records"].copy() if not dist.empty else pd.DataFrame()
    if rows.empty:
        return no_rows_figure(snapshot, "tone_distribution")

    order = ["calming", "stable", "escalating"]
    labels_pretty = {"calming": "Calming (< -0.15)", "stable": "Stable (-0.15 to +0.15)", "escalating": "Escalating (> +0.15)"}
    rows["dimension"] = pd.Categorical(rows["dimension"], categories=order, ordered=True)
    rows = rows.sort_values("dimension").dropna(subset=["dimension"])

    fig = go.Figure(
        go.Pie(
            labels=[labels_pretty[d] for d in rows["dimension"]],
            values=rows["value"],
            hole=0.55,
            marker={"colors": [TONE_COLORS[d] for d in rows["dimension"]], "line": {"color": "white", "width": 2}},
            textinfo="percent",
            hovertemplate="%{label}<br>Notices: %{value:,.0f} (%{percent})<extra></extra>",
        )
    )
    fig.update_layout(
        template=TEMPLATE,
        title={"text": "Distribution of Volcanic Activity Tone Across Notices"},
        margin={"l": 20, "r": 20, "t": 105, "b": 75},
        showlegend=True,
    )
    fig.add_annotation(
        text=f"Source: {SOURCE}", xref="paper", yref="paper", x=0, y=0, yshift=-45,
        xanchor="left", yanchor="top", showarrow=False, font={"size": 11, "color": "#666666"},
    )
    return fig


def weekly_tone_figure(snapshot: GoldSnapshot, series_dim: str = "ALL") -> go.Figure:
    """Weekly activity tone trajectory (domain lexicon vs. VADER)."""
    tone = snapshot.section("tone_weekly")
    if tone.empty:
        return no_rows_figure(snapshot, "tone_weekly")

    lex = tone[(tone["metric"] == "lexicon_mean") & (tone["dimension"] == series_dim)].sort_values("period")
    if lex.empty:
        return no_rows_figure(snapshot, "tone_weekly")

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=lex["period"],
            y=lex["value"],
            name="Volcanic Lexicon Tone",
            mode="lines",
            line={"color": TONE_COLORS["escalating"], "width": 2.2},
            hovertemplate="Week of %{x|%b %d, %Y}<br>Lexicon tone: %{y:+.2f}<extra></extra>",
        )
    )

    if series_dim == "ALL":
        vad = tone[(tone["metric"] == "vader_mean") & (tone["dimension"] == "ALL")].sort_values("period")
        if not vad.empty:
            fig.add_trace(
                go.Scatter(
                    x=vad["period"],
                    y=vad["value"],
                    name="VADER Baseline",
                    mode="lines",
                    line={"color": TONE_COLORS["stable"], "width": 1.6, "dash": "dash"},
                    hovertemplate="Week of %{x|%b %d, %Y}<br>VADER score: %{y:+.2f}<extra></extra>",
                )
            )

    fig.add_hline(y=0.15, line={"color": TONE_COLORS["escalating"], "dash": "dot", "width": 1})
    fig.add_hline(y=-0.15, line={"color": TONE_COLORS["calming"], "dash": "dot", "width": 1})
    fig.update_yaxes(range=[-1, 1])
    subtitle = "All U.S. Volcanoes" if series_dim == "ALL" else f"Volcano ID {series_dim}"
    return style_figure(
        fig,
        f"Weekly Activity Tone Trajectory ({subtitle})",
        "Week (UTC)",
        "Activity tone score (-1 calming, +1 escalating)",
        SOURCE).update_layout(margin={"t": 95}, legend={"y": 1.04})


def top_keywords_figure(snapshot: GoldSnapshot, ngram_type: str = "unigram") -> go.Figure:
    """Horizontal bar chart of top terms colored by their mean activity tone."""
    kw = snapshot.section("top_keywords")
    if kw.empty:
        return no_rows_figure(snapshot, "top_keywords")

    prefix = f"{ngram_type}:"
    sub = kw[kw["dimension"].str.startswith(prefix)].copy()
    if sub.empty:
        return no_rows_figure(snapshot, "top_keywords")

    piv = sub.pivot(index="dimension", columns="metric", values="value").reset_index()
    if "frequency" not in piv.columns:
        return no_rows_figure(snapshot, "top_keywords")
    if "mean_lexicon_tone" not in piv.columns:
        piv["mean_lexicon_tone"] = 0.0

    piv["term"] = piv["dimension"].str.replace(prefix, "", regex=False)
    piv = piv.sort_values("frequency", ascending=False).head(15).sort_values("frequency", ascending=True)

    def tone_bucket(val: float) -> str:
        if val > 0.15:
            return "escalating"
        if val < -0.15:
            return "calming"
        return "stable"

    piv["bucket"] = piv["mean_lexicon_tone"].apply(tone_bucket)

    fig = go.Figure()
    for bucket, label in [("calming", "Calming"), ("stable", "Stable"), ("escalating", "Escalating")]:
        part = piv[piv["bucket"] == bucket]
        if part.empty:
            continue
        fig.add_trace(
            go.Bar(
                x=part["frequency"],
                y=part["term"],
                orientation="h",
                name=f"{label} tone",
                marker_color=TONE_COLORS[bucket],
                customdata=part["mean_lexicon_tone"],
                hovertemplate="Term: %{y}<br>Mentions: %{x:,.0f}<br>Mean tone: %{customdata:+.2f}<extra></extra>",
            )
        )

    fig.update_layout(barmode="stack", yaxis={"categoryorder": "total ascending"})
    label_kind = "Keywords (Unigrams)" if ngram_type == "unigram" else "Phrases (Bigrams)"
    return style_figure(
        fig,
        f"Top 15 {label_kind} Colored by Activity Tone",
        "Frequency in notice summaries (mentions)",
        "Term",
        SOURCE).update_layout(margin={"t": 95}, legend={"y": 1.04})


def source_comparison_figure(snapshot: GoldSnapshot) -> go.Figure:
    """Side-by-side comparison of volume, text length, and tone across HANS API vs. Scraping."""
    sc = snapshot.section("source_comparison")
    if sc.empty:
        return no_rows_figure(snapshot, "source_comparison")

    piv = sc.pivot(index="dimension", columns="metric", values="value").reset_index()
    piv["label"] = piv["dimension"].map(SOURCE_LABELS).fillna(piv["dimension"])

    fig = make_subplots(
        rows=1,
        cols=2,
        subplot_titles=("Mean Bulletin Length (characters)", "Mean Activity Tone Score"),
        horizontal_spacing=0.14,
    )

    if "mean_text_length" in piv.columns:
        fig.add_trace(
            go.Bar(
                x=piv["label"],
                y=piv["mean_text_length"],
                name="Mean text length",
                marker_color=CATEGORICAL[0],
                customdata=piv.get("records", pd.Series([0] * len(piv))),
                hovertemplate="%{x}<br>Mean length: %{y:,.0f} chars<br>Records: %{customdata:,.0f}<extra></extra>",
            ),
            row=1,
            col=1,
        )

    if "lexicon_mean" in piv.columns:
        fig.add_trace(
            go.Bar(
                x=piv["label"],
                y=piv["lexicon_mean"],
                name="Volcanic lexicon tone",
                marker_color=TONE_COLORS["escalating"],
                hovertemplate="%{x}<br>Lexicon tone: %{y:+.2f}<extra></extra>",
            ),
            row=1,
            col=2,
        )
    if "vader_mean" in piv.columns:
        fig.add_trace(
            go.Bar(
                x=piv["label"],
                y=piv["vader_mean"],
                name="VADER tone",
                marker_color=TONE_COLORS["stable"],
                hovertemplate="%{x}<br>VADER tone: %{y:+.2f}<extra></extra>",
            ),
            row=1,
            col=2,
        )

    fig.update_yaxes(title_text="Characters", row=1, col=1)
    fig.update_yaxes(title_text="Tone score (-1 to +1)", range=[-1, 1], row=1, col=2)
    fig.update_layout(
        template=TEMPLATE,
        barmode="group",
        title={"text": "Official HANS API vs. Scraped Volcano Updates Comparison"},
        margin={"l": 60, "r": 20, "t": 125, "b": 95}, legend={"orientation": "h", "yanchor": "bottom", "y": 1.14, "x": 0},
    )
    fig.add_annotation(
        text=f"Source: {SOURCE}", xref="paper", yref="paper", x=0, y=0, yshift=-75,
        xanchor="left", yanchor="top", showarrow=False, font={"size": 11, "color": "#666666"},
    )
    return fig


def volume_activity_figure(snapshot: GoldSnapshot, source_filter: str = "ALL") -> go.Figure:
    """Weekly record volume per source with peak activity week highlighted."""
    vol = snapshot.section("volume_activity")
    if vol.empty:
        eq = snapshot.section("earthquakes_weekly").sort_values("period")
        if eq.empty:
            return no_rows_figure(snapshot, "volume_activity")
        fig = go.Figure(
            go.Scatter(
                x=eq["period"], y=eq["value"], mode="lines", name="Earthquakes",
                line={"color": CATEGORICAL[0], "width": 2},
            )
        )
        return style_figure(fig, "Weekly Seismic Activity Near U.S. Volcanoes", "Week (UTC)", "Events (records)", SOURCE)

    if source_filter != "ALL":
        vol = vol[vol["dimension"] == source_filter]
    if vol.empty:
        return no_rows_figure(snapshot, "volume_activity")

    fig = go.Figure()
    sources = sorted(vol["dimension"].unique())
    for idx, src in enumerate(sources):
        part = vol[vol["dimension"] == src].sort_values("period")
        label = SOURCE_LABELS.get(src, src)
        color = CATEGORICAL[idx % len(CATEGORICAL)]
        fig.add_trace(
            go.Scatter(
                x=part["period"],
                y=part["value"],
                mode="lines",
                name=label,
                line={"color": color, "width": 2},
                hovertemplate=f"{label}<br>Week of %{{x|%b %d, %Y}}: %{{y:,.0f}} records<extra></extra>",
            )
        )
        if len(sources) == 1 or src == "usgs_hans_notices_api":
            peak = part.loc[part["value"].idxmax()]
            fig.add_trace(
                go.Scatter(
                    x=[peak["period"]],
                    y=[peak["value"]],
                    mode="markers+text",
                    name=f"Peak ({label})",
                    text=[f"Peak: {int(peak['value']):,}"],
                    textposition="top center",
                    marker={"color": TONE_COLORS["escalating"], "size": 9},
                    showlegend=False,
                )
            )

    return style_figure(
        fig,
        "Weekly Monitoring Volume & Activity Peaks by Source",
        "Week (UTC)",
        "Ingested records (count)",
        SOURCE).update_layout(margin={"t": 95}, legend={"y": 1.04})


def _tone_series_options(snapshot: GoldSnapshot) -> list[dict]:
    options = [{"label": "All U.S. Volcanoes (Network Average)", "value": "ALL"}]
    tone = snapshot.section("tone_weekly")
    if tone.empty:
        return options
    dims = sorted([d for d in tone["dimension"].dropna().unique() if d != "ALL"])
    for d in dims:
        options.append({"label": f"Elevated Volcano (VNUM {d})", "value": str(d)})
    return options


def create_app(gold_folder: Path | None = None) -> Dash:
    app = Dash(__name__, title=TITLE, external_stylesheets=[dbc.themes.FLATLY])

    def page():
        snapshot = load_gold(KIND, gold_folder)
        return dbc.Container([
            header(TITLE, ANGLE, snapshot),
            dbc.Container([
                refresh_controls(),
                # Story Layer 1: Overview (Narrative Card + Tone Distribution Donut)
                dbc.Row([
                    dbc.Col(html.Div(id="narrative-card", children=build_narrative_card(snapshot)), lg=7),
                    dbc.Col(chart_card("tone-donut"), lg=5),
                ], className="g-3 mb-3"),
                # Interactive Story Filter Bar
                dbc.Card(
                    dbc.CardBody(
                        dbc.Row([
                            dbc.Col([
                                html.Label("Activity Tone Scope", className="fw-bold small mb-1"),
                                dcc.Dropdown(
                                    id="tone-scope-filter",
                                    options=_tone_series_options(snapshot),
                                    value="ALL",
                                    clearable=False,
                                ),
                            ], md=4),
                            dbc.Col([
                                html.Label("Volume Activity Source", className="fw-bold small mb-1"),
                                dcc.Dropdown(
                                    id="volume-source-filter",
                                    options=[
                                        {"label": "All Monitored Sources", "value": "ALL"},
                                        {"label": "USGS HANS Notices (API)", "value": "usgs_hans_notices_api"},
                                        {"label": "USGS Volcano Updates (Scraping)", "value": "usgs_volcano_updates_scraping"},
                                        {"label": "USGS Earthquakes (FDSN / Feed)", "value": "usgs_earthquakes"},
                                    ],
                                    value="ALL",
                                    clearable=False,
                                ),
                            ], md=4),
                            dbc.Col([
                                html.Label("Top Terms Granularity", className="fw-bold small mb-1"),
                                dcc.Dropdown(
                                    id="keyword-type-filter",
                                    options=[
                                        {"label": "Single Keywords (Unigrams)", "value": "unigram"},
                                        {"label": "Two-Word Phrases (Bigrams)", "value": "bigram"},
                                    ],
                                    value="unigram",
                                    clearable=False,
                                ),
                            ], md=4),
                        ], className="g-3")
                    ),
                    className="shadow-sm mb-3",
                ),
                # Story Layer 2: Weekly Shift (Tone Trend + Volume Activity with Peaks)
                dbc.Row([
                    dbc.Col(chart_card("tone-trend"), lg=6),
                    dbc.Col(chart_card("volume-activity"), lg=6),
                ], className="g-3 mb-3"),
                # Story Layer 3: Keywords Colored by Tone + Source Comparison
                dbc.Row([
                    dbc.Col(chart_card("top-keywords"), lg=6),
                    dbc.Col(chart_card("source-comparison"), lg=6),
                ], className="g-3 mb-4"),
                # Part 1: Volcano Activity Indicator (map, ranking by region, history per volcano)
                indicator_panel.layout(),
            ], fluid=True),
        ], fluid=True, className="px-0")

    app.layout = page
    indicator_panel.register_callbacks(app, gold_folder)

    @app.callback(
        Output("last-updated", "children"),
        Output("narrative-card", "children"),
        Output("tone-donut", "figure"),
        Output("tone-trend", "figure"),
        Output("volume-activity", "figure"),
        Output("top-keywords", "figure"),
        Output("source-comparison", "figure"),
        Output("tone-scope-filter", "options"),
        Input("refresh-button", "n_clicks"),
        Input("refresh-interval", "n_intervals"),
        Input("tone-scope-filter", "value"),
        Input("volume-source-filter", "value"),
        Input("keyword-type-filter", "value"),
    )
    def refresh(_clicks, _ticks, tone_scope, vol_source, kw_type):
        current = load_gold(KIND, gold_folder)
        return (
            last_updated_text(current),
            build_narrative_card(current),
            tone_donut_figure(current),
            weekly_tone_figure(current, tone_scope or "ALL"),
            volume_activity_figure(current, vol_source or "ALL"),
            top_keywords_figure(current, kw_type or "unigram"),
            source_comparison_figure(current),
            _tone_series_options(current),
        )

    return app


def main() -> None:
    create_app().run(
        host=os.getenv("DASH_HOST", "127.0.0.1"),
        port=int(os.getenv("STORYTELLING_PORT", DEFAULT_PORT)),
        debug=False,
    )


if __name__ == "__main__":
    main()
