# Dashboards (Workshop #4)

Two independent Plotly Dash apps. Both read **only** the latest Gold Parquet file of their kind with `pd.read_parquet()`; they never touch Silver, raw JSON, the project database or an API.

| App | Port | Reads | For |
|---|---|---|---|
| `governance_app.py` | 8050 | `datalake_gold/governance_*.parquet` | the data team: quality KPIs of the pipeline |
| `storytelling_app.py` | 8051 | `datalake_gold/storytelling_*.parquet` | the emergency-management analyst of Workshop 1 |

## Run

```bash
# with the rest of the stack
docker compose up -d --build                     # or only: docker compose up -d --build governance-dashboard storytelling-dashboard

# on the host (pip install --user -r requirements.txt first)
python -m dashboard.governance_app               # http://localhost:8050
python -m dashboard.storytelling_app             # http://localhost:8051
```

Run the host commands from the repo root. `GOVERNANCE_PORT` / `STORYTELLING_PORT` change the port (in Docker, the host side of the mapping, set in `.env`); `DASH_HOST` and `DATALAKE_ROOT` change where the app listens and where it looks for `datalake_gold/`.

## Structure

```
dashboard/
├── common/
│   ├── gold.py          # load_gold(kind) -> GoldSnapshot: newest readable file, cached until a newer one appears
│   ├── theme.py         # Okabe-Ito palette, tone and alert colours, style_figure()
│   └── layout.py        # header(), refresh_controls(), chart_card(), empty_figure()
├── governance_app.py    # create_app(), one figure builder per chart
├── storytelling_app.py
├── indicator_panel.py   # VAI panel of the storytelling app: map, ranking by region, history per volcano
├── requirements.txt     # dependencies of the dashboard image only
├── Dockerfile           # python:3.11-slim; the code is mounted, not copied
└── .dockerignore        # the build only sends requirements.txt
```

## Add a chart

1. Write a figure builder `my_chart_figure(snapshot: GoldSnapshot) -> go.Figure` in the app module. Take the rows with `snapshot.section("<section>")`, return `no_rows_figure(snapshot, "<section>")` when they are missing, and finish with `style_figure(fig, title, x_title, y_title, SOURCE)` so the chart has a title, axis titles with units and the source note.
2. Put `chart_card("my-chart")` in the layout.
3. Add `Output("my-chart", "figure")` to the `refresh` callback and return `my_chart_figure(current)`.
4. Test the builder on a small Gold file (`tests/dashboard_helpers.py`), like `tests/test_dashboard_apps.py`.

Colours: tone always uses `TONE_COLORS` (calming green, stable grey, escalating red), alert levels `ALERT_LEVEL_COLORS`, aviation codes `COLOR_CODE_COLORS`, everything else `CATEGORICAL`.
