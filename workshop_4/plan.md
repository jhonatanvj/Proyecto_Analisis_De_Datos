# Workshop 4 plan: work split for Team 6

Status: draft 2026-11-24. Workshop 4 document: *Interactive Dashboards with Plotly Dash* (2026-I). The final grade also uses the CourseProject rubric, so its Workshop 4 items (Volcano Activity Indicator, recent events map, historical-versus-current panel, data-health panel) are included.

## What the workshop asks for

1. End-to-end run from a clean state (Bronze → Silver → Gold), fix and document the integration issues found, kept apart from the Workshop 3 improvements.
2. `dashboard/governance_app.py` at `localhost:8050` for a technical audience: KPI cards (total records, null rate, duplicate rate, schema compliance), null rate per field (horizontal bars, sorted), volume over time per source, outlier rate per numeric field, a quality summary table with current value and threshold.
3. `dashboard/storytelling_app.py` at `localhost:8051` for the functional user: tone distribution (donut), tone trend, top keywords coloured by tone, API vs. scraping comparison, volume activity, a narrative summary card.
4. Both apps read **only** the latest Gold Parquet files with `pd.read_parquet()`, start independently with one command, and refresh (automatic or a button) to pick up a newer Gold file.
5. Every chart: descriptive title, labelled axes with units, legend where needed, source annotation. One colourblind-friendly palette for both apps; tone always green (calming) / grey (stable) / red (escalating). Header with project title, topic angle, team name and data last-updated time. Readable at 1080p, no horizontal scroll.
6. Deliverables: report PDF, `workshop_4/` with the PDF and full-page screenshots, Airflow screenshots of the three DAGs, README with complete setup, updated `requirements.txt`, tag `v1.0-workshop4`. A recorded walkthrough is optional but recommended.

## Plotting libraries

* **Plotly** in the dashboards, because the workshop requires hover, zoom and filters, which static images cannot give.
* **Seaborn** in the notebooks and the report figures (the professor's recommendation): each part has one Seaborn notebook whose figures go into its report sections.

## Where we start from

| Done | Missing |
|---|---|
| Bronze → Silver → Gold DAGs running in Docker; Gold written on 2026-11-24 | No `dashboard/` folder; `dash`, `plotly`, `dash-bootstrap-components`, `seaborn` not in the requirements |
| Governance Gold: `volume_total`, `volume_weekly`, `null_rate`, `duplicate_rate`, `schema_compliance`, `quarantine_rate`, `outlier_rate`, `text_length`, `freshness`, `ingestion_frequency` | Governance thresholds are not stored anywhere; no watermark age per source (US4) |
| Storytelling Gold: `tone_weekly`, `top_keywords`, `source_comparison`, `earthquakes_weekly`, `earthquakes_baseline`, `alerts_breakdown` | No tone distribution (records per escalating / stable / calming); no volume per source and week for the narrative |
| VAI weights and thresholds in `config/sources.yaml` (`indicator:`), formula in `docs/project_definition.tex` | VAI not computed; no map data (volcano coordinates + current alert) in Gold |

## Gold contract (unchanged)

Both files keep the Workshop 3 long format (`section, metric, dimension, period, value, unit, generated_utc`); every new chart only needs a new `section`. Text such as a volcano name goes in `dimension`; the dashboards pivot the rows they need.

## Split

Part 1 starts first and hands over a shared layer and two running app skeletons, so Parts 2 and 3 only fill in charts. After the hand-off the three parts run in parallel on their own branches. Each part has the same shape: Gold additions → dashboard → tests → Seaborn notebook → report sections.

### Part 1 — Integration, shared layer and the activity indicator (Jhonatan)

1. **Shared layer** `dashboard/common/`: Gold loader (latest `governance_*` / `storytelling_*` file, cached, reloaded when a newer file appears), colour palette (colourblind-friendly, tone colours, alert colour codes), header component (title, topic angle, team, last updated from `generated_utc`), chart helpers (title, axis units, source annotation), refresh button + interval.
2. **App skeletons**: `governance_app.py` (8050) and `storytelling_app.py` (8051) that start, show the header and an empty grid; `dash`, `plotly`, `dash-bootstrap-components`, `seaborn` added to the requirements; one command per app (`python -m dashboard.governance_app`) and two compose services so `docker compose up` also starts both dashboards.
3. **Hand-off guide** `workshop_4/handoff_parts_2_3.md`: how to run the apps, the shared helpers, the Gold sections available and how to add one, how to test a chart.
4. **Volcano Activity Indicator in Gold** (`gold/indicator.py`, sections `vai_current`, `vai_weekly`, `volcano_map`): current R/S/H components and score per volcano, which components were available (US8), weekly history from 2010 with S and H only (US7), coordinates + current alert level and colour code for the map (US1).
5. **Indicator panel in the storytelling app**: map of US volcanoes coloured by alert level, VAI ranking filterable by region (US6), historical-versus-current panel per volcano (US3, US5, US7), with the "monitoring aid, not a forecast" disclaimer.
6. **Integration**: full run from a clean state, fixes, `workshop_4/integration_adjustments.md`; README with complete setup (Docker, trigger DAGs, both dashboards); fresh-clone test; tag `v1.0-workshop4` once Parts 2 and 3 are merged.
7. **Seaborn notebook** `notebooks/05_vai_validation.ipynb`: weight sensitivity, hit and false-alarm rates around US eruptions since 2010, case studies (Kilauea 2018, Mauna Loa 2022, Great Sitkin 2021, Ahyi 2025).
8. **Report sections**: *Executive Summary*, *Integration Adjustments*, *Volcano Activity Indicator*, *Reproducibility Evidence*.

### Part 2 — Governance dashboard (Julián Pulido)

1. **Gold additions** in `governance.py`: one threshold per KPI (values in `config/sources.yaml`, `governance.thresholds`) written as rows next to the KPI; data health per source for US4 (last successful file, watermark age, rejected records).
2. **`governance_app.py`**: KPI cards with a pass/warn colour against the threshold, null rate per field (horizontal bars sorted by severity), volume over time per source, outlier rate per numeric field, quality summary table (KPI, value, threshold, status), data-health panel; filter by source and topic.
3. **Tests** on a small Gold fixture: the app builds its layout and every chart callback returns a figure with title and axis labels.
4. **Seaborn notebook** `notebooks/06_governance_figures.ipynb`: static versions of the KPI charts for the report.
5. **Report sections**: *Governance Dashboard Documentation* (layout, component list, KPI mapping, design rationale), *Visualization Design Decisions* (chart types, palette, layout); **assembles the final PDF** from everyone's sections.

### Part 3 — Storytelling dashboard (Juan Sebastián Rangel)

1. **Write the story flow first**: overview → what changed this week → which volcanoes and why, in plain language for the emergency-management analyst.
2. **Gold additions** in `storytelling.py`: tone distribution (records per escalating / stable / calming), records per week and source for the volume chart, the few numbers the narrative card needs (e.g. most escalating volcano this week, change vs. last week).
3. **`storytelling_app.py`**: tone donut, tone trend (weekly), top keywords coloured by tone, API (HANS) vs. scraping (Volcano Updates) comparison, volume activity with peaks marked, narrative summary card generated from Gold; filter by region / observatory. Places the Part 1 indicator panel in the page.
4. **Tests** on a small Gold fixture, like Part 2.
5. **Seaborn notebook** `notebooks/07_storytelling_figures.ipynb`: tone and keyword figures for the report.
6. **Report sections**: *Storytelling Dashboard Documentation* (layout, components, user-story mapping, narrative rationale), *Challenges and Resolutions*; dashboard screenshots and the optional walkthrough video.

## Working rules

* One branch per part (`workshop-4` for Part 1, `w4-governance`, `w4-storytelling`), merged into `main` through a pull request reviewed by another member. Parts 2 and 3 branch off `main` once the Part 1 hand-off is merged.
* Dashboards read Gold only: no Silver, raw JSON, database or API calls from `dashboard/`.
* Code comments and documentation in English; no keys or `.env` in commits.
* `pytest -q` (and the Spark tests in Docker) must pass before opening a pull request.
* Each member must be able to explain their part in the final defense (individual grade).
