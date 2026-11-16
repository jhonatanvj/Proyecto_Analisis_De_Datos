# Workshop 2 design: Bronze/Silver medallion pipeline on Airflow

Status: draft for team approval (2026-11-16).

## Intent

Workshop 2 asks for a dockerized Apache Airflow stack, a Bronze ingestion DAG (one API source plus one web-scraping source, raw JSON in `datalake_bronze/`) and a Silver processing DAG triggered by a `FileSensor` that cleans the data and writes Parquet to `datalake_silver/`. We build that on top of the Workshop 1 work (US volcano scope, source clients, canonical mapping, data-quality findings) so the later workshops (Gold with PySpark, Dash dashboards) can reuse it.

## Goals

- `docker compose up` starts Airflow (webserver on `localhost:8080`, scheduler, triggerer, PostgreSQL metadata DB) with the project folders mounted as volumes.
- `airflow/dags/bronze_ingestion_dag.py` (TaskFlow API) extracts from the USGS APIs and scrapes the USGS Volcano Updates page, validates each payload and writes one JSON file per source and run.
- `airflow/dags/silver_processing_dag.py` (TaskFlow API + `FileSensor`) detects unprocessed Bronze files, applies the preprocessing pipeline with pandas and writes Parquet files that point back to their Bronze file.
- Every preprocessing step is justified by a finding observed in the Workshop 1 samples.
- At least 3 Bronze JSON files and 3 Silver Parquet files from real runs, a notebook that reads Silver with pandas, a `workshop_2/` report and an updated README.

## Non-goals (this workshop)

- PostgreSQL/PostGIS canonical database and `ingest.py --mode historical|incremental` (kept as they are; not required by the Workshop 2 document).
- FDSN multi-year backfill (365k rows). The client stays; a backfill DAG can come later.
- Gold layer, PySpark, dashboards (Workshops 3 and 4).
- Sentiment analysis: the text preprocessing prepares tokens, it does not score them.

## Anti-goals

- Scraping a page that blocks bots. The Smithsonian weekly report HTML answers 403 with a bot challenge, so it is not used.
- Imputing values that would invent science (VEI, magnitude, alert level). Missing values in those fields are flagged, not filled.
- A Silver DAG that re-processes the same Bronze file or writes the same record twice.

## Approaches considered

| | Approach | Trade-off |
|---|---|---|
| **A (chosen)** | APIs (USGS HANS, USGS earthquake feed, GVP WFS) + scraping of `https://www.usgs.gov/programs/VHP/volcano-updates` with BeautifulSoup | Meets the "API + web scraping" requirement literally and gives real text for the NLP step. Risk: HTML layout changes; mitigated by a schema check and a recorded HTML fixture in tests. |
| B | APIs + scraping each volcano's USGS status page | More text, but ~70 requests per run and fragile pages. |
| C | APIs only, justify the missing scraping | Least work, leaves a requirement unmet and nothing for the text pipeline. |

Recon on 2026-11-16: the Volcano Updates page is server-rendered HTML (6.6 MB), `robots.txt` allows `/programs/`, and it contains one block per elevated volcano (`div.usgs-vol-up-elevated`: name, alert level, colour code, "as of" time, synopsis, HANS notice URL) plus the latest full notice per observatory (`div.usgs-tabs-main` with ids `avo`, `calvo`, `cvo`, `hvo`, `nmi`, `yvo`: VNUM, issued time, notice number, summary and activity text).

## Sources per layer

| Source id | Kind | Bronze content | Silver topic |
|---|---|---|---|
| `usgs_hans` | API (JSON) | `volcano/getMonitoredVolcanoes` response | `alert_notices` |
| `usgs_feed` | API (GeoJSON) | `all_week.geojson` response | `earthquakes` |
| `gvp_volcanoes`, `gvp_eruptions` | API (WFS GeoJSON) | layer responses, fetched only when no Bronze file exists for the current month | `volcanoes`, `eruptions` |
| `usgs_volcano_updates` | Web scraping (HTML) | records extracted by BeautifulSoup + SHA-256 of the HTML | `volcano_updates` |

The weekly RSS and GDACS stay optional and out of the DAGs.

## Folder layout

```
airflow/dags/bronze_ingestion_dag.py
airflow/dags/silver_processing_dag.py
airflow/Dockerfile, airflow/requirements.txt
docker-compose.yml
datalake_bronze/  datalake_silver/  datalake_gold/   (git-ignored except .gitkeep and a few samples)
src/volcano_pipeline/sources/volcano_updates.py      (scraper)
src/volcano_pipeline/bronze/                          (envelope writer, payload validation)
src/volcano_pipeline/silver/                          (per-topic cleaning, schemas, text, state)
notebooks/02_silver_exploration.ipynb
workshop_2/                                           (report, preprocessing spec, samples)
```

DAG files stay thin: they call functions in `src/volcano_pipeline`, which are unit-tested without Airflow.

## Bronze

- File name: `<source_id>_YYYYMMDD_HHMMSS.json` (UTC), as the workshop and the course contract require.
- Content: a JSON envelope `{source_id, retrieved_utc, url, params, http_status, content_sha256, record_count, payload}`. `payload` is the response JSON exactly as parsed (for the scraper: the extracted records). One file is self-describing, and `content_sha256` is computed on the original bytes.
- Validation task: per-source schema check (expected top-level keys, list of records, required fields present in a sample). A failed check fails the task with a structured log line; nothing is written.
- Idempotency: if `content_sha256` equals that of the latest Bronze file of the same source, the run logs `unchanged` and writes no file. Re-running the DAG for the same moment cannot create a duplicate file.
- Schedule: `@hourly` (HANS hourly polling from the Workshop 1 contract; the feed is a 7-day window, so hourly loses nothing). Retries: 3 with exponential backoff at task level, on top of the HTTP retries already in `sources/http.py`.

## Silver

- Trigger: schedule `@hourly` at minute 15; first task is a `FileSensor` subclass (`NewBronzeFileSensor`) watching `datalake_bronze/*.json` that only returns true when a file is not yet in the Silver manifest. `mode="reschedule"`, `poke_interval=60` s, `timeout=40` min, `soft_fail=True` (no new file → the run is skipped, not failed). Rationale for these values goes in the report.
- State: `datalake_silver/_state/manifest.json` (processed Bronze files) and `_state/seen.parquet` (`record_id → content_hash`). They make the DAG idempotent and give cross-batch deduplication.
- Output: `<topic>_YYYYMMDD_HHMMSS.parquet`, only new or changed records, with lineage columns `bronze_file`, `bronze_sha256`, `processed_utc`. Rejected rows go to `datalake_silver/quarantine/rejected_YYYYMMDD_HHMMSS.parquet` with `source_id`, `reason`, `payload` (JSON string) and `bronze_file`.
- Library: pandas + pyarrow (small batches, already used in Workshop 1; Polars is unnecessary at this size). Parquet: columnar, typed, compressed, and readable by PySpark in Workshop 3.

### Preprocessing (first version, each line tied to a Workshop 1 finding)

| Step | Rule |
|---|---|
| Schema enforcement | An explicit pyarrow schema per topic; columns cast to it; missing required field → quarantine. |
| Types | Timestamps to UTC, stored in Parquet as `timestamp[us, UTC]` (Spark 3 cannot read nanosecond Parquet timestamps) (epoch ms for the feed, epoch s for HANS, partial dates for GVP with `time_precision`); numeric fields to float; alert level, colour code, magType, network and status to `category`. |
| Nulls | Feed `mag` null → keep, `mag_missing` flag. GVP VEI null → keep NULL + flag. Partial GVP dates → first day of period + precision; BCE → NULL time, keep year. `Last_Eruption_Year = 'Unknown'` → NULL. HANS `vnum` null/placeholder or `notice_identifier` null → quarantine with the Workshop 1 reasons. FDSN-style quality fields (gap, rms…) stay NULL; they are not used in rules. |
| Validation ranges | Latitude [-90, 90], longitude normalised to [-180, 180), depth [-5, 800] km (negative depths are valid at volcanoes), magnitude [-2, 10]; non-earthquake `type` → quarantine `non_tectonic_type`. |
| Outliers | Magnitude and depth: IQR (1.5×) and robust z-score flags per batch (`is_outlier_*`). Flagged, not removed: at volcanoes, swarms and shallow events are the signal. |
| Deduplication | Within a batch: keep the latest `updated` per `record_id`. Across batches: skip records whose `(record_id, content_hash)` is already in `seen.parquet`; a changed hash produces a new row (revision). |
| Text (scraped notices) | Strip HTML and `&nbsp;`, Unicode NFKC normalisation (keeps `Kīlauea`, removes stray bytes), lowercase, remove URLs and punctuation, regex tokenisation, English stop words (a fixed list in `config/`, no runtime downloads), domain keyword flags (`eruption`, `ash`, `lava`, `seismicity`, `deformation`, `gas`, `plume`). Output columns: `text_clean`, `tokens`, `token_count`, plus parsed `alert_level`, `color_code` and `vnum`. |

## Docker stack

- `postgres:16` (Airflow metadata), `airflow-init`, `airflow-webserver` (8080), `airflow-scheduler`, `airflow-triggerer`. Apache Airflow 2.10 with `LocalExecutor`: tasks run as subprocesses of the scheduler, so a Celery worker plus Redis is not needed for a single machine. This is explained in the report since the workshop text mentions a worker.
- Custom image `FROM apache/airflow:2.10.x-python3.11` plus `airflow/requirements.txt` (requests, beautifulsoup4, lxml, pandas, pyarrow, PyYAML), installed with the Airflow constraints file.
- Volumes: `airflow/dags`, `airflow/logs`, `datalake_bronze`, `datalake_silver`, `datalake_gold`, `src` (on `PYTHONPATH`) and `config`.
- Secrets and the User-Agent come from `.env`; `.env.example` gets the new variables.

## Testing

pytest with recorded fixtures only: a trimmed copy of the Volcano Updates HTML, the existing HANS fixture and a small feed sample. Tests cover the scraper parser, the Bronze envelope and validation, each Silver cleaning function, the manifest/dedup logic (processing the same Bronze file twice adds nothing) and the sensor's "new file" check.

## Left to the builder

Exact file splits inside `src/volcano_pipeline/silver/`, log field names, and the Airflow 2.10 patch version.

## Needs the team

- Running Docker Desktop, triggering the DAGs over several hours, and taking the Airflow UI screenshots for the report.
- Reviewing and submitting the PDF.
- Risk: the repo lives inside OneDrive. Bind mounts usually work, but if OneDrive locks files, clone the repo outside OneDrive to run Docker.
