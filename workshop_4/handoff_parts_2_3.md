# Workshop 4 hand-off: how to build Parts 2 and 3

For Julián (Part 2, governance dashboard) and Juan Sebastián (Part 3, storytelling dashboard). Part 1 left both Dash apps running with the shared header, colours, refresh and one example chart each; your job is to add the charts of your dashboard and the Gold rows they need. Plan and split: [`plan.md`](plan.md). Workshop 3 recipe for Gold sections: [`../workshop_3/handoff_parts_2_3.md`](../workshop_3/handoff_parts_2_3.md).

## 1. Set up your clone (once)

```bash
git fetch && git checkout main && git pull
git checkout -b w4-governance                  # Part 2   (Part 3: w4-storytelling)
docker compose up -d --build                   # rebuilds the images and also starts both dashboards
```

Python on your machine (for the tests, the notebooks and running an app while you edit it), as in README section 5. Use Python 3.11 and a virtual environment, so the project's packages never clash with other installs:

```bash
python -m venv .venv
.venv\Scriptsctivate                         # macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt                # now also dash, plotly, dash-bootstrap-components, seaborn
python -m pytest -q                            # Spark tests are skipped unless Java 8/11/17 is installed
```

Open http://localhost:8050 (governance) and http://localhost:8051 (storytelling). If they say "no Gold data yet", trigger `gold_processing` in Airflow (http://localhost:8080) and press *Refresh data*. If 8050 or 8051 is already taken on your machine, set `GOVERNANCE_PORT` / `STORYTELLING_PORT` in `.env` and run `docker compose up -d` again.

While you work on a chart, run the app on your machine from the repo root (with the `.venv` active): `python -m dashboard.governance_app` (or `storytelling_app`). Stop the Docker service first (`docker compose stop governance-dashboard`) or the port is busy. Restart the app to see code changes. The Docker services mount the code, so `docker compose restart governance-dashboard` shows your changes there too.

## 2. What Part 1 gives you

| Piece | Where | Use |
|---|---|---|
| `load_gold(kind)` → `GoldSnapshot` | `dashboard/common/gold.py` | newest `governance_*` / `storytelling_*` file; `snapshot.section("null_rate")` gives that section's rows; `snapshot.generated_utc` the run time. Cached until a newer file appears, so calling it on every refresh is cheap; an unreadable file is logged and the previous one is used |
| `style_figure(fig, title, x_title, y_title, source)` | `dashboard/common/theme.py` | template, title, axis titles with units, the "Source: ..." note. Every chart goes through it |
| `TONE_COLORS`, `ALERT_LEVEL_COLORS`, `COLOR_CODE_COLORS`, `CATEGORICAL` | `dashboard/common/theme.py` | the only colours to use (Okabe-Ito, colourblind-friendly); tone is always calming green / stable grey / escalating red. The lexicon score is +1 escalating, -1 calming, so a high score is red |
| `header()`, `refresh_controls()`, `chart_card(id)`, `no_rows_figure(snapshot, section)`, `empty_figure(msg)` | `dashboard/common/layout.py` | page header with the last-updated time, refresh button + 5-minute interval, one chart in a card, placeholder that says whether Gold or only that section is missing |
| Example chart | `governance_app.records_per_topic_figure`, `storytelling_app.weekly_tone_figure` | copy this pattern for each chart. The page is built by a function (`page()` in `create_app`), so a browser reload always shows the newest Gold |
| Test helpers | `tests/dashboard_helpers.py` | `write_gold(folder, kind, stamp, rows)` writes a small Gold file to test a figure builder |

## 3. Add a chart (the pattern)

```python
def null_rate_figure(snapshot: GoldSnapshot) -> go.Figure:
    rows = snapshot.section("null_rate").sort_values("value")
    if rows.empty:
        return no_rows_figure(snapshot, "null_rate")
    fig = go.Figure(go.Bar(x=rows["value"], y=rows["dimension"], orientation="h"))
    return style_figure(fig, "Share of empty values per field", "Empty values (%)", "Field", SOURCE)
```

Then `chart_card("null-rate")` in the layout, one more `Output("null-rate", "figure")` in `refresh()`, and a test in `tests/test_dashboard_apps.py` (or your own test file) that checks the title, the axis titles and the number of bars on a small Gold file. Dash callbacks can also take filters (a `dcc.Dropdown` as an extra `Input`), which the workshop asks for (hover, zoom, filter).

If a chart needs numbers Gold does not have yet, add a section in `src/volcano_pipeline/gold/governance.py` or `storytelling.py` exactly as in Workshop 3 (function → list at the bottom → Spark test), run `gold_processing` and refresh. **Dashboards never compute from Silver.**

## 4. What Gold has today (files of 2026-11-24)

**Governance** (`governance_20261124_233802.parquet`, 2,437 rows)

| Section | Rows | Metrics |
|---|---|---|
| `volume_total` | 7 | `records` per topic |
| `volume_weekly` | 1,764 | `records` per source and week |
| `null_rate` | 124 | `pct_null` per field |
| `duplicate_rate` | 21 | `pct_exact_copies`, `pct_real_revisions`, `pct_repeated_before_dedup`, `pct_same_value_copies`, `records` |
| `schema_compliance` | 14 | `files`, `pct_files_conforming` |
| `quarantine_rate` | 15 | `pct_quarantined`, `records_by_reason` |
| `outlier_rate` | 4 | `pct_outlier`, `pct_outlier_silver_flags` |
| `text_length` | 441 | `min/mean/median/max_chars`, `pct_encoding_errors` |
| `freshness` | 8 | `hours_since_last` per source |
| `ingestion_frequency` | 39 | `expected_per_day`, `hours_with_file`, `pct_of_expected` |

**Storytelling** (`storytelling_20261124_233950.parquet`, 7,868 rows)

| Section | Rows | Metrics |
|---|---|---|
| `tone_weekly` | 5,552 | `lexicon_mean` (dimension `ALL` or a vnum with an elevated alert), `vader_mean`, `alert_level_mean`; weeks 2009-12-28 → 2026-11-23. Lexicon tone: +1 escalating, -1 calming |
| `top_keywords` | 90 | `frequency`, `mean_lexicon_tone` per keyword or bigram |
| `source_comparison` | 8 | `records`, `lexicon_mean`, `vader_mean`, `mean_text_length` for HANS (API) vs. Volcano Updates (scraping) |
| `earthquakes_weekly` | 883 | `earthquakes` per week |
| `earthquakes_baseline` | 1,311 | `weekly_count`, `baseline_p90`, `baseline_ratio` per volcano |
| `alerts_breakdown` | 24 | volcanoes per observatory and alert level, per region and colour code |
| `vai_current`, `vai_weekly`, `volcano_map`, `eruption_history` | added by Part 1 (Workshop 4) | Volcano Activity Indicator: score, level and components per volcano, weekly history since 2010, map attributes (`dimension` = `vnum|name|region|observatory`), eruptions since 1900. Shown by `dashboard/indicator_panel.py`; rules in [`vai_indicator.md`](vai_indicator.md) |

## 5. Who owns what

| Part | Chart ids (suggested) | Gold to add |
|---|---|---|
| 2: governance (8050) | `kpi-cards`, `null-rate`, `volume-over-time`, `outlier-rate`, `quality-table`, `data-health` | a threshold per KPI (values in `config/sources.yaml`, `governance.thresholds`); per source: last successful file, watermark age, rejected records (US4) |
| 3: storytelling (8051) | `narrative-card`, `tone-donut`, `tone-trend`, `top-keywords`, `source-comparison`, `volume-activity` | records per tone label (escalating / stable / calming); records per week and source; the few numbers the narrative card quotes |
| 1: indicator panel in 8051 | `volcano-map`, `vai-ranking`, `volcano-history` | `vai_current`, `vai_weekly`, `volcano_map` (Part 1 adds them) |

Keep the example chart or replace it, your call. Part 1's indicator panel is already in `storytelling_app.py` as one row, `indicator_panel.layout()`, below the example chart; its callbacks live in `dashboard/indicator_panel.py`. Put your story above it and leave that row and the `register_callbacks` line as they are.

## 6. Checklist before the pull request (from the workshop)

- [ ] Every chart has a descriptive title, axis titles with units, a legend where needed and the source note (`style_figure`).
- [ ] Tone colours are green / grey / red everywhere; no other palette.
- [ ] Bars instead of a pie for more than three categories; lines for time series.
- [ ] Hover, zoom and at least one filter work and help the reader.
- [ ] Readable at 1920×1080 with no horizontal scroll.
- [ ] Storytelling: plain words, no jargon. Would the emergency-management analyst understand each chart without help?
- [ ] The app starts with an empty `datalake_gold/` (shows "no Gold data yet").
- [ ] `python -m pytest -q` passes; the Spark tests pass in Docker if you changed Gold.
- [ ] Your Seaborn notebook (`notebooks/06_governance_figures.ipynb` / `07_storytelling_figures.ipynb`) and your report sections are in the branch.
- [ ] Full-page screenshot of your dashboard with real Gold data in `workshop_4/figures/`.
