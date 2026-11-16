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
| `usgs_fdsn` | USGS FDSN Event Web Service (ComCat) | historical | CSV | Public domain, credit USGS |
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
| Silver (clean Parquet per topic + quarantine) | **running**: `silver_processing` DAG, FileSensor-triggered | `datalake_silver/<topic>_YYYYMMDD_HHMMSS.parquet` |
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

## Run the pipeline (Docker)

Requirements: Docker Desktop with at least 4 GB of memory for Docker.

```bash
cp .env.example .env              # set HTTP_USER_AGENT with a team e-mail
docker compose up -d --build      # postgres, airflow-init, webserver, scheduler, triggerer
```

1. Open http://localhost:8080 (user `airflow`, password `airflow` unless changed in `.env`).
2. Unpause `bronze_ingestion` and `silver_processing`. Bronze runs every hour at minute 0; Silver starts at minute 15 and waits up to 40 min for new Bronze files.
3. To run them once by hand: trigger `bronze_ingestion`, wait for it to finish, then trigger `silver_processing`. From a terminal:

```bash
docker compose exec airflow-scheduler airflow dags test bronze_ingestion
docker compose exec airflow-scheduler airflow dags test silver_processing
```

Outputs appear on the host in `datalake_bronze/` and `datalake_silver/`. Explore them with [`notebooks/02_silver_exploration.ipynb`](notebooks/02_silver_exploration.ipynb). `docker compose down` stops the stack; add `-v` to also delete the Airflow metadata database.

If the repository lives in a OneDrive/Dropbox folder and Docker reports file access errors, clone it to a folder outside the synced directory.

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
│   └── storage/               # raw landing zone (Workshop #1)
├── airflow/                   # Dockerfile, requirements, dags/ (bronze_ingestion, silver_processing)
├── docker-compose.yml         # Airflow stack
├── datalake_bronze/ datalake_silver/ datalake_gold/   # data lake layers (git-ignored)
├── data/raw/                  # raw landing zone (git-ignored)
├── sql/                       # schema definitions (draft DDL)
├── dashboard/                 # Dash app (Workshop #4)
├── tests/ and tests/fixtures/ # pytest + recorded responses
├── notebooks/01_source_profiling.ipynb, 02_silver_exploration.ipynb
├── scripts/fetch_samples.py   # downloads the Workshop #1 evidence samples
├── docs/                      # ER diagram, architecture, report sources
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
python ingest.py --mode historical [--start 2010-01-01 --end 2025-12-31]   # Workshop #2
python ingest.py --mode incremental                                        # Workshop #3
python ingest.py --mode status                                             # watermarks, last runs, row counts
```

## Workshop deliverables

* **Workshop #1:** project definition report ([`docs/project_definition.tex`](docs/project_definition.tex) — build with `pdflatex project_definition.tex`, run twice) and [`workshop_1/`](workshop_1/): mapping table, ingestion contract, samples, source status log. Also the notebook, the ER diagram ([`docs/er_diagram.png`](docs/er_diagram.png)) and the draft DDL ([`sql/001_schema_draft.sql`](sql/001_schema_draft.sql)).
* **Workshop #2:** [`workshop_2/`](workshop_2/): report ([`report.tex`](workshop_2/report.tex)), preprocessing pipeline ([`preprocessing.md`](workshop_2/preprocessing.md)), design, architecture diagram and output samples (Bronze JSON, Silver Parquet, quarantine). Code: [`docker-compose.yml`](docker-compose.yml), [`airflow/dags/bronze_ingestion_dag.py`](airflow/dags/bronze_ingestion_dag.py), [`airflow/dags/silver_processing_dag.py`](airflow/dags/silver_processing_dag.py), `src/volcano_pipeline/{bronze,silver}`. Notebook: [`notebooks/02_silver_exploration.ipynb`](notebooks/02_silver_exploration.ipynb).
