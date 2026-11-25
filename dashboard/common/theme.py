"""Colours and the Plotly template shared by both dashboards.

The palette is Okabe-Ito, which stays distinguishable for the common types of colour blindness.
Tone colours follow the workshop rule (green / grey / red), alert colours follow the USGS meaning
of each level and aviation colour code.
"""
from __future__ import annotations

import plotly.graph_objects as go
import plotly.io as pio

# Okabe-Ito, in the order categories receive them
CATEGORICAL = ["#0072B2", "#E69F00", "#009E73", "#CC79A7", "#56B4E9", "#D55E00", "#F0E442", "#000000"]

# activity tone of the USGS notices (Workshop 3). The score runs from -1 (calming, green) to
# +1 (escalating, red): a high score is bad news, so "positive sentiment" means calming here.
TONE_COLORS = {"calming": "#009E73", "stable": "#999999", "escalating": "#D55E00"}

UNASSIGNED = "#BBBBBB"
# USGS volcano alert level (ground hazard)
ALERT_LEVEL_COLORS = {"NORMAL": "#009E73", "ADVISORY": "#F0E442", "WATCH": "#E69F00",
                      "WARNING": "#D55E00", "UNASSIGNED": UNASSIGNED}
# aviation colour code: the names are the colours, so they keep their meaning
COLOR_CODE_COLORS = {"GREEN": "#009E73", "YELLOW": "#F0E442", "ORANGE": "#E69F00",
                     "RED": "#D55E00", "UNASSIGNED": UNASSIGNED}

TEMPLATE = "volcano"
FONT = "Segoe UI, Helvetica Neue, Arial, sans-serif"

pio.templates[TEMPLATE] = go.layout.Template(layout=go.Layout(
    font={"family": FONT, "size": 13, "color": "#222222"},
    title={"font": {"size": 16}, "x": 0.01, "xanchor": "left"},
    colorway=CATEGORICAL,
    paper_bgcolor="white",
    plot_bgcolor="white",
    # automargin widens the margins for long tick labels (e.g. category names on horizontal bars)
    xaxis={"showgrid": True, "gridcolor": "#EEEEEE", "zeroline": False, "linecolor": "#999999",
           "automargin": True, "title": {"standoff": 12}},
    yaxis={"showgrid": True, "gridcolor": "#EEEEEE", "zeroline": False, "linecolor": "#999999",
           "automargin": True, "title": {"standoff": 12}},
    legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "x": 0},
    hoverlabel={"font": {"family": FONT}},
    # the bottom margin holds the tick labels, the x-axis title and the source note
    margin={"l": 60, "r": 20, "t": 70, "b": 90},
))


def style_figure(fig: go.Figure, title: str, x_title: str, y_title: str, source: str) -> go.Figure:
    """Apply the shared template, the title, both axis titles (with units) and the source note."""
    fig.update_layout(template=TEMPLATE, title={"text": title},
                      xaxis_title=x_title, yaxis_title=y_title)
    # placed in pixels under the x axis (not as a fraction of the plot), so it stays inside the
    # bottom margin whatever height a chart gets
    fig.add_annotation(text=f"Source: {source}", xref="paper", yref="paper", x=0, y=0, yshift=-55,
                       xanchor="left", yanchor="top", showarrow=False,
                       font={"size": 11, "color": "#666666"})
    return fig
