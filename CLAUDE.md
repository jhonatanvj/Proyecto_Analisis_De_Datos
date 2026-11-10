# CLAUDE.md — project context for Claude Code

## What this project is
Semester project for **Data Analysis Programming 2026-II** (Universidad Distrital, Prof. Carlos Andrés Sierra).
**Team 6 — Volcanic Activity Monitoring.**

| Code | Name |
|---|---|
| 20231020006 | Juan Rangel |
| 20231020055 | Jhonatan David Vergara Jiménez (main user of this repo) |
| 20231020064 | Julián Pulido |

The goal is a dockerized multi-source ETL that integrates historical and continuously updated volcano data into one canonical PostgreSQL/PostGIS model. The pipeline is orchestrated by Airflow and feeds a Plotly Dash dashboard (`localhost:8050`) that shows a **Volcano Activity Indicator (VAI)**. The VAI is a monitoring aid, never an eruption forecast.

Course milestones:

| Milestone | Content | Weight |
|---|---|---|
| Workshop 1 | Sources, profiling, mapping, contract, repo | 10% |
| Workshop 2 | Docker stack, schema, `--mode historical` | 10% |
| Workshop 3 | `--mode incremental`, watermarks, event matching | 10% |
| Workshop 4 | Analytics, indicator, dashboard | 10% |
| Final | Integration, report, defense | 30% |

The course PDFs (CourseProject, TeamsDistribution, Workshop_1..4) live in the team's claude.ai Project, not in this repo. Ask the user for them if you need exact requirements.

**Language:** the user writes in Spanish, so answer in Spanish. All repo documentation, code comments and reports must be in **English** (course rule).

## Mandatory course contract (do not break)
- CLI: `python ingest.py --mode historical [--start --end]`, `--mode incremental`, `--mode status`.
- Both modes are idempotent: running either twice creates no duplicates.
- `incremental` fetches only the interval last watermark → now. The watermark advances only in the same transaction as the data commit.
- Raw landing zone: files exactly as received, named `<source>_YYYYMMDD_HHMMSS.<ext>`, in `data/raw/` (git-ignored).
- Common tables: `source`, `ingestion_run`, `ingestion_state`, `event` (event_id, source, event_type, timestamp UTC, lat, lon, severity + unit, raw_reference + content hash), `event_group`, `rejected_record`. The domain tables are added on top.
- Configuration lives outside code (`config/sources.yaml`, `.env`; never commit `.env` or keys). Logging is structured, tests use pytest with recorded fixtures (no live calls in tests).
- At least 2 historical sources (different formats, at least 5 years) and at least 2 incremental sources.

## DECISION 2026-11-09: scope changed from GLOBAL to UNITED STATES
Workshop 1 was first drafted with a **global** scope. The team then decided to switch to a **US-only** scope. The files below **still describe the global version** and must be updated:

1. `config/sources.yaml`
   - Set `project.geographic_scope: united_states`.
   - Restrict volcanoes to `Country == 'United States'` (about 160 volcanoes in Alaska, Hawaii, the Cascades, Yellowstone, CNMI and others).
   - Lower `spatial.min_magnitude`, e.g. to 1.0–1.5. US networks are complete at low magnitudes; measure the volume first with `scripts/fetch_samples.py --estimate-volume`.
   - Promote `usgs_hans` (USGS HANS API: current alert level + aviation colour code) from optional to a **main incremental source**. Verify its real endpoints and fields first: https://volcanoes.usgs.gov/hans-public/api/volcano/ .
   - Keep or demote the Weekly Volcanic Activity Report RSS. Only a few US volcanoes appear in it per week.
2. `scripts/fetch_samples.py`
   - Use US volcanoes for the FDSN sample instead of the global list (Kilauea 332010 and Mauna Loa 332020 are fine; add Alaskan and Cascades volcanoes such as Great Sitkin, Spurr, Mount St. Helens).
   - Download HANS samples.
3. `README.md`, `workshop_1/mapping.md` / `mapping.csv` and `workshop_1/ingestion_contract.md`
   - Add the HANS mapping: `event_type = 'alert_notice'`, severity = alert level NORMAL/ADVISORY/WATCH/WARNING plus aviation code GREEN/YELLOW/ORANGE/RED.
   - Add a domain table such as `volcano_alert`.
4. Indicator (`config/sources.yaml` → `indicator`)
   - Consider using the official alert level / colour code as the "reported" component (R).
5. Report (Claude Docs, editable): https://claude.ai/artifact/G8Q6e9uweNijKrdxnueQ9J
   - Update the sections on scope, sources, user (e.g. state emergency-management / FEMA analyst) and risks.
   - Answer the scope comment there.

## Workshop 1 status
Done:
- Repo structure.
- `config/sources.yaml`.
- `src/volcano_pipeline/` with HTTP clients for GVP WFS, USGS FDSN, the USGS feed and the weekly RSS, plus the canonical mappers and the raw landing zone.
- `ingest.py` stub (`status` works).
- 9 passing unit tests.
- Profiling notebook (`notebooks/01_source_profiling.ipynb`; tested only with synthetic data).
- Mapping, ingestion contract, draft DDL (`sql/001_schema_draft.sql`), ER and architecture diagrams (`docs/`).
- Report draft.

**Not done:** no real data has been downloaded yet; the first session had no network access to the APIs. Next steps:
1. `pip install -r requirements.txt`; `cp .env.example .env` (set HTTP_USER_AGENT with a contact e-mail).
2. Apply the US-scope changes above.
3. `python scripts/fetch_samples.py`. Then `--poll` at least 1 h later, and again after the Wednesday 23:00 UTC weekly report.
4. Execute the notebook (`jupyter nbconvert --to notebook --execute --inplace notebooks/01_source_profiling.ipynb`).
   - The GVP field names in the code (`Volcano_Number`, `ExplosivityIndexMax`, `StartDateYear`…) were written without access to the API. Check them against the real columns the notebook prints and fix the code if needed.
5. Paste the real evidence (response snippets, poll comparison, data-quality examples, candidate pairs, volume) into the "To fill" boxes of the report, then export the report to PDF.
6. Push to GitHub and add the URL to the report.

## Key design decisions (keep unless the team changes them)
- Volcano = static geographic entity; vnum (GVP volcano number) is the join key everywhere.
- Earthquakes are linked to volcanoes within a radius (30 km default) in a many-to-many `volcano_earthquake` table.
- Canonical ids: `usgs:<id>` (shared by FDSN and the feed, so it is the natural dedup key), `gvp_eruption:<n>`, `gvp_weekly:<vnum>:<week_start>`.
- Severity keeps its own scale per event type (VEI, magnitude + magType, report status / alert level). There is no cross-type conversion.
- GVP partial dates map to the first day of the known period, with a `time_precision` column. BCE dates get `event_time = NULL`.
- VAI = weighted mix of reported activity (R), the seismic percentile vs the volcano's own baseline (S) and eruptive state (H). Weights and thresholds live in `config/sources.yaml`.
- Cite GVP with the database version and DOI (v5.4.0, doi:10.5479/si.GVP.VOTW5-2026.5.4 at the time of Workshop 1).

## Useful commands
```bash
python ingest.py --mode status
pytest -q
python scripts/fetch_samples.py [--poll | --estimate-volume]
```
