# Volcanic Activity Monitoring System

**Team 6 · Data Analysis Programming 2026-II · Universidad Distrital Francisco José de Caldas**
Professor: Eng. Carlos Andrés Sierra, M.Sc.

| Student code | Name |
|---|---|
| 20231020006 | Juan Sebastián Rangel Pinto |
| 20231020055 | Jhonatan David Vergara Jiménez |
| 20231020064 | Julián Pulido |

## Hazard

Volcanic activity: unrest and eruptions of the volcanoes of the **United States** (Alaska, Hawaii, the Cascades, California, Yellowstone, the Northern Mariana Islands and American Samoa). The system integrates the **eruption history** and **static characteristics** of each volcano with the **official USGS alert level and aviation colour code** and **nearby seismicity**. It produces a **Volcano Activity Indicator** per volcano. The indicator is a monitoring and correlation aid, **not an eruption forecast**.

* **Geographic scope:** United States: the 165 Holocene volcanoes that GVP lists with `Country = 'United States'`, plus the volcanoes monitored by USGS that GVP keeps outside the Holocene list (e.g. Yellowstone). The USGS HANS alert levels exist only for US volcanoes, so a US scope is the one covered by all four sources.
* **Historical window:** earthquakes from 2010-01-01 to today. Eruption history covers the whole Holocene record; time-series analysis starts in 1960.
* **Current situation:** the current USGS alert level / aviation colour code of each volcano and the earthquakes (M ≥ 1.0) of the last 7 days within 30 km of it.

## Sources

| ID | Source | Role | Format | Licence / attribution |
|---|---|---|---|---|
| `gvp_volcanoes`, `gvp_eruptions` | Smithsonian GVP *Volcanoes of the World* (WFS) | historical | GeoJSON | Cite the database version and DOI (v5.4.0, 7 Aug 2026, doi:10.5479/si.GVP.VOTW5-2026.5.4) |
| `usgs_fdsn` | USGS FDSN Event Web Service (ComCat) | historical (one-off backfill DAG) | GeoJSON (CSV in Workshop 1) | Public domain, credit USGS |
| `usgs_hans` | USGS HANS public API (alert level + aviation colour code) | incremental (hourly) | JSON | Public domain, credit USGS Volcano Hazards Program |
| `usgs_feed` | USGS real-time GeoJSON feed `all_week` | incremental (minutes) | GeoJSON | Public domain, credit USGS |
| `usgs_volcano_updates` | USGS Volcano Updates page (**web scraping**, BeautifulSoup) | incremental (hourly) | HTML → JSON records | Public domain, credit USGS Volcano Hazards Program |
| `gvp_weekly_rss` | Smithsonian / USGS Weekly Volcanic Activity Report | optional (weekly context) | RSS XML | Cite GVP Weekly Report |
| `gdacs_vo` | GDACS (VO) | optional, disabled | JSON | see `config/sources.yaml` |

None of these sources needs an API key. Full details (URLs, limits, watermarks, duplicate keys) are in [`config/sources.yaml`](config/sources.yaml) and [`workshop_1/ingestion_contract.md`](workshop_1/ingestion_contract.md).

## Pipeline status

| Layer | Status | Where |
|---|---|---|
| Bronze (raw JSON per source and run) | **running**: `bronze_ingestion` DAG, hourly | `datalake_bronze/<source>_YYYYMMDD_HHMMSS.json` |
| Historical earthquakes (FDSN, 2010 → today) | **ready**: `bronze_backfill_fdsn` DAG, run by hand once | `datalake_bronze/usgs_fdsn_*.json` → Silver `earthquakes_*` |
| Silver (clean Parquet per topic + quarantine) | **running**: `silver_processing` DAG, FileSensor-triggered | `datalake_silver/<topic>_YYYYMMDD_HHMMSS.parquet` |
| Project database (PostgreSQL + PostGIS, canonical model) | **loaded** from Silver by the `load_to_database` task of the Silver DAG and by `ingest.py` ([details](docs/database_handoff.md)) | service `project-db`, `sql/`, `src/volcano_pipeline/storage/loader.py` |
| Gold (governance + analytical summaries, PySpark) | planned (Workshop #3) | `datalake_gold/` |
| Dashboard (Plotly Dash, `localhost:8050`) | planned (Workshop #4) | `dashboard/` |

## Architecture

Medallion pipeline on Apache Airflow 2.10 (Docker Compose, LocalExecutor). The diagram source is [`workshop_2/architecture.mmd`](workshop_2/architecture.mmd), and the full description is in the Workshop 2 report ([`workshop_2/report.tex`](workshop_2/report.tex)). The Workshop 1 design diagram is [`docs/architecture.png`](docs/architecture.png).

```
sources (HANS, USGS feed, GVP WFS, USGS Volcano Updates page)
   -> bronze_ingestion DAG (@hourly: extract -> validate -> persist)  -> datalake_bronze/*.json
   -> silver_processing DAG (FileSensor -> pandas preprocessing)     -> datalake_silver/*.parquet (+ quarantine/)
   -> Gold with PySpark (Workshop 3) -> Dash dashboard (Workshop 4)
```

## Getting started on your own machine

Everything below was tested on Windows 11 with Docker Desktop. It works the same on macOS and Linux; the differences are noted.

### 1. Prerequisites

| Tool | Version | Used for |
|---|---|---|
| Git | any | clone the repository |
| Docker Desktop (or Docker Engine + Compose v2) | Compose 2.24+ | the Airflow stack. Give Docker **at least 4 GB of RAM** (Settings → Resources) |
| Python | 3.11 | tests, notebooks and the local runner (not needed just to run the DAGs) |

### 2. Clone and configure

```bash
git clone https://github.com/jhonatanvj/Proyecto_Analisis_De_Datos.git
cd Proyecto_Analisis_De_Datos
git checkout workshop-2              # until it is merged into main
cp .env.example .env                 # Windows PowerShell: Copy-Item .env.example .env
```

Edit `.env`:
* `HTTP_USER_AGENT`: put a team e-mail. USGS and GVP ask clients to identify themselves.
* **Linux only:** set `AIRFLOW_UID` to your user id (`id -u`), otherwise Airflow cannot write to `airflow/logs/` and the `datalake_*` folders. Leave `50000` on Windows and macOS.
* `.env` is git-ignored; never commit it.

Do not clone into a OneDrive, Dropbox or iCloud folder if you can avoid it: syncing can lock files that Docker is writing.

### 3. Start Airflow

```bash
docker compose up -d --build         # first build: 3-5 min (downloads the Airflow image)
docker compose ps                    # wait until webserver and scheduler are "healthy" (1-2 min)
```

Open http://localhost:8080 and log in with user `airflow`, password `airflow`. To use other credentials, set `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD` in `.env` before the first start.

### 4. Run the DAGs

In the UI, switch on (unpause) **`bronze_ingestion`** and **`silver_processing`**. Bronze runs every hour at minute 0 and Silver at minute 15. To run them right away, use the ▶ (trigger) button: first Bronze, wait until it is green, then Silver. The same from a terminal:

```bash
docker compose exec airflow-scheduler airflow dags test bronze_ingestion
docker compose exec airflow-scheduler airflow dags test silver_processing
```

Results appear on your machine:

| Folder | Content |
|---|---|
| `datalake_bronze/` | `<source>_YYYYMMDD_HHMMSS.json`: raw payload of each source and run (a source whose content did not change writes no new file) |
| `datalake_silver/` | `<topic>_YYYYMMDD_HHMMSS.parquet`: clean data per topic (`earthquakes`, `alert_notices`, `volcanoes`, `eruptions`, `volcano_updates`) |
| `datalake_silver/quarantine/` | `rejected_YYYYMMDD_HHMMSS.parquet`: rejected records with the reason |
| `datalake_silver/_state/` | processed-file manifest and dedup index (do not edit by hand) |

The first Bronze run downloads about 14 MB; after that, the GVP catalogue is fetched only once a month. The feed and HANS responses are small.

**Historical earthquakes (once).** The hourly DAGs only bring the last 7 days of earthquakes. To load the history since 2010, run **`bronze_backfill_fdsn`** by hand: in the UI, click ▶ → *Trigger DAG w/ config*. Leave the defaults (all US volcanoes, 2010-01-01 → today) or set `vnums` (e.g. `[332010]` for Kilauea) and `start`/`end`. Run `bronze_ingestion` at least once first, because the backfill needs the volcano positions.
* For each volcano it asks USGS how many events there are. Busy volcanoes such as Kilauea are split into date ranges under the 20,000-events-per-request limit.
* Each range is downloaded as `usgs_fdsn_*.json`, at most 4 requests at the same time.
* The next `silver_processing` run turns the files into `earthquakes_*.parquet`.
* The full load is about 365,000 events and 350 MB of Bronze JSON, and takes tens of minutes. Try one volcano first.

### 5. Python environment (tests, notebooks, local runner)

```bash
python -m venv .venv
.venv\Scripts\activate                # macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
pytest -q                            # ~80 tests, no network needed
jupyter notebook notebooks/02_silver_exploration.ipynb
```

To run the pipeline once **without Docker** (same code as the DAGs; useful to debug a source):

```bash
python scripts/run_pipeline_local.py                           # Bronze (all sources) + Silver
python scripts/run_pipeline_local.py --only bronze --sources usgs_hans
python scripts/run_pipeline_local.py --root ../lake_test       # write to another folder
python scripts/run_pipeline_local.py --fdsn-backfill --vnums 332010 --start 2024-01-01   # FDSN history, then Silver
```

By default it writes to the same `datalake_*` folders as Docker. Do not run it while the DAGs are running, or pass `--root`.

### 6. Stop, restart, reset

```bash
docker compose stop                  # pause everything (keeps data and Airflow history)
docker compose up -d                 # start again
docker compose down                  # remove containers (data in datalake_* stays)
docker compose down -v               # also delete the Airflow metadata DB (run history, users)
```

To reprocess Silver from the Bronze files, pause the DAGs and delete everything in `datalake_silver/` except `.gitkeep`. The next Silver run rebuilds it.

### 7. Project database (PostgreSQL + PostGIS)

`docker compose up -d` also starts the `project-db` service. On its first start it creates the schema from `sql/` (`001_schema_draft.sql`, `002_silver_indicators.sql` with the Silver quality flags, lineage columns, the `volcano_update` table and the `silver_quality_indicators` view, and `003_loader_state.sql` with the list of Silver files already loaded).

The last task of the `silver_processing` DAG (`load_to_database`) loads every new Silver file into the database: one transaction per file with the upserts, the `ingestion_run` row and the watermark in `ingestion_state`. To load what is already on disk without Airflow (for example the first time):

```bash
python ingest.py --mode historical --skip-fetch      # Silver files not in the database yet
python ingest.py --mode status                       # watermarks, last runs, row counts
```

```bash
docker compose up -d project-db                                   # only the database
docker compose exec project-db psql -U volcano -d volcano -c "\dt"
```

Connect from pgAdmin, DBeaver or any SQL client: host `localhost`, port `POSTGRES_PORT` (5432, or 5433 if a local PostgreSQL already uses 5432), database `volcano`, user `volcano`, password `POSTGRES_PASSWORD` from `.env`. Step by step, how to apply schema changes and next steps: [`docs/database_handoff.md`](docs/database_handoff.md).

### Troubleshooting

| Symptom | Fix |
|---|---|
| `port is already allocated` on 8080 or 5432 | Another program uses the port. Stop it, or change the left side of `ports:` in `docker-compose.yml`. For 5432, set `POSTGRES_PORT` in `.env` |
| Webserver keeps restarting, or the UI is very slow | Give Docker more memory (4 GB+) |
| A DAG shows "Broken DAG" | `docker compose exec airflow-scheduler airflow dags list-import-errors` |
| Task logs | UI → DAG → Grid → task → Logs, or the `airflow/logs/` folder |
| `Permission denied` writing `datalake_*` (Linux) | Set `AIRFLOW_UID=$(id -u)` in `.env`, then `docker compose down && docker compose up -d` |
| `pytest` fails on `airflow/logs/...` | Run it from the repo root, where `pytest.ini` limits collection to `tests/` |
| Silver run is "skipped" | Normal: no new Bronze file in that hour (the sources did not change) |
| Apple Silicon: PostGIS image warning | `postgis/postgis` has no ARM image; it runs under emulation, which is fine for development |

## Repository structure

```
├── ingest.py                  # CLI: --mode historical | incremental | status
├── config/sources.yaml        # one entry per source (URL, format, schedule, watermark, licence)
├── src/volcano_pipeline/
│   ├── config.py              # loads YAML + .env
│   ├── sources/               # HTTP clients + volcano_updates scraper
│   ├── bronze/                # extraction, payload validation, JSON envelope writer
│   ├── silver/                # schemas, cleaning per topic, text, outliers, dedup state, runner
│   ├── normalization/         # canonical mapping, geo helpers
│   └── storage/               # raw landing zone, database connection and Silver -> PostgreSQL loader
├── airflow/                   # Dockerfile, requirements, dags/ (bronze_ingestion, silver_processing, bronze_backfill_fdsn)
├── docker-compose.yml         # Airflow stack + PostGIS project database
├── datalake_bronze/ datalake_silver/ datalake_gold/   # data lake layers (git-ignored)
├── data/raw/                  # raw landing zone (git-ignored)
├── sql/                       # DDL (001 draft, 002 Silver indicators, 003 loader state), loaded by the project-db container
├── dashboard/                 # Dash app (Workshop #4)
├── tests/ and tests/fixtures/ # pytest + recorded responses
├── notebooks/01_source_profiling.ipynb, 02_silver_exploration.ipynb
├── scripts/                   # fetch_samples.py (W1), run_pipeline_local.py, collect_w2_samples.py (W2)
├── docs/                      # ER diagram, architecture, W1 report, database_handoff.md
├── workshop_1/                # mapping, ingestion contract, samples, source status log
└── workshop_2/                # design, preprocessing spec, report, architecture, samples
```

## Workshop #1 scripts (local Python)

```bash
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env                                     # set HTTP_USER_AGENT with a team e-mail

python scripts/fetch_samples.py                          # 1st poll: all sources -> workshop_1/samples
#   ... wait at least 1 hour ...
python scripts/fetch_samples.py --poll                   # 2nd poll of the incremental sources
#   ... on a later day (new HANS notices / weekly report) ...
python scripts/fetch_samples.py --poll                   # 3rd poll
python scripts/fetch_samples.py --estimate-volume        # FDSN counts for all US volcanoes (~3 min)

jupyter nbconvert --to notebook --execute --inplace notebooks/01_source_profiling.ipynb
python ingest.py --mode status
pytest -q
```

## CLI contract

```bash
python ingest.py --mode historical [--start 2010-01-01 --end 2025-12-31]  # GVP + FDSN -> Bronze -> Silver -> database
python ingest.py --mode incremental                                        # HANS, feed, Volcano Updates -> ... -> database
python ingest.py --mode status                                             # watermarks, last runs, row counts
python ingest.py --mode historical --skip-fetch                            # no API calls: only load pending files
```

Both loading modes are idempotent: Silver keeps only new or changed records, the loader skips Silver files already listed in `silver_load_file` and rewrites a row only when its content hash changed. Details: [`docs/database_handoff.md`](docs/database_handoff.md).

## Workshop deliverables

* **Workshop #1:** project definition report ([`docs/project_definition.tex`](docs/project_definition.tex) — build with `pdflatex project_definition.tex`, run twice) and [`workshop_1/`](workshop_1/): mapping table, ingestion contract, samples, source status log. Also the notebook, the ER diagram ([`docs/er_diagram.png`](docs/er_diagram.png)) and the draft DDL ([`sql/001_schema_draft.sql`](sql/001_schema_draft.sql)).
* **Workshop #2:** [`workshop_2/`](workshop_2/): report ([`report.tex`](workshop_2/report.tex)), preprocessing pipeline ([`preprocessing.md`](workshop_2/preprocessing.md)), design, architecture diagram and output samples (Bronze JSON, Silver Parquet, quarantine). Code: [`docker-compose.yml`](docker-compose.yml), [`airflow/dags/bronze_ingestion_dag.py`](airflow/dags/bronze_ingestion_dag.py), [`airflow/dags/silver_processing_dag.py`](airflow/dags/silver_processing_dag.py), `src/volcano_pipeline/{bronze,silver}`. Notebook: [`notebooks/02_silver_exploration.ipynb`](notebooks/02_silver_exploration.ipynb).
