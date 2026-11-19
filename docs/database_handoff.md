# Project database: how it is loaded and what is left

Status on 2026-11-19: **loaded**. The `project-db` container starts with the rest of the stack and creates the schema. The Silver DAG loads every new Silver file into it (task `load_to_database`), and `ingest.py` does the same from the command line. First full load: 169 volcanoes, 1,281 eruptions, 291,439 earthquakes, 54 HANS notices, 9 Volcano Updates and 2,019 rejected records, in about 90 s.

## Why we need it

The course project document (final delivery, 30%) asks for:

* PostgreSQL + PostGIS with the common tables `source`, `ingestion_run`, `ingestion_state`, `event`, `event_group` and `rejected_record`, plus volcano domain tables.
* `python ingest.py --mode historical | incremental | status`, idempotent, with a watermark per source that advances in the same transaction as the data.
* A dashboard on `localhost:8050` that reads only from the database or its views.

The Workshop 3 document asks for a Gold layer of Parquet files built with PySpark. The plan is to keep both: Silver Parquet → Gold Parquet for Spark, and Silver Parquet → database for `ingest.py` and the dashboard.

## What exists

| Piece | Where | State |
|---|---|---|
| Base schema (DDL) | [`sql/001_schema_draft.sql`](../sql/001_schema_draft.sql) | Workshop 1: 12 tables + view `volcano_current_alert`. Idempotent since Workshop 3 |
| Silver indicators (DDL) | [`sql/002_silver_indicators.sql`](../sql/002_silver_indicators.sql) | Workshop 2: quality flags, lineage, `volcano_update`, view `silver_quality_indicators`, `source` rows. Idempotent |
| Loader state (DDL) | [`sql/003_loader_state.sql`](../sql/003_loader_state.sql) | Table `silver_load_file` (Silver files already loaded) and two indexes. Idempotent |
| Workshop 3 sources | [`sql/004_workshop3_sources.sql`](../sql/004_workshop3_sources.sql) | `source` rows for `usgs_hans_notices` and `usgs_hans_volcanoes`. Idempotent |
| HANS notice texts | [`sql/005_volcano_notice.sql`](../sql/005_volcano_notice.sql) | Table `volcano_notice` (Silver topic `notices`): 16,535 notices 2010-2026 with summary, cleaned text, tokens, alert level, volcano and region. Own table, not a child of `event`, because region-wide notices have no coordinates |
| Schema migrations | service `db-migrate`, [`scripts/db_migrate.sh`](../scripts/db_migrate.sh) | Applies new or changed `sql/*.sql` on every `docker compose up`; table `schema_migration` |
| Loader | [`src/volcano_pipeline/storage/loader.py`](../src/volcano_pipeline/storage/loader.py), connection in [`db.py`](../src/volcano_pipeline/storage/db.py) | Silver Parquet → database, see below |
| ER diagram | [`docs/er_diagram.png`](er_diagram.png) (source `er_diagram.mmd`) | Matches 001; does not show the 002 columns yet |
| Mapping source field → table/column | [`workshop_1/mapping.md`](../workshop_1/mapping.md), [`mapping.csv`](../workshop_1/mapping.csv) | Workshop 1 |
| Ingestion contract (upserts, watermarks, retries) | [`workshop_1/ingestion_contract.md`](../workshop_1/ingestion_contract.md) | Workshop 1 |
| PostGIS container | service `project-db` in [`docker-compose.yml`](../docker-compose.yml) | Starts with `docker compose up -d`; the schema comes from `db-migrate` |
| Connection settings | `POSTGRES_*` in `.env` (copy of `.env.example`) | Host `localhost`, port 5432 (5433 if a local PostgreSQL uses 5432), db/user `volcano`. Airflow uses `project-db:5432` (set in `docker-compose.yml`) |
| Clean data to load | `datalake_silver/<topic>_*.parquet` (topics `volcanoes`, `eruptions`, `earthquakes`, `alert_notices`, `volcano_updates`) and `datalake_silver/quarantine/` | Running every hour |
| CLI | [`ingest.py`](../ingest.py) | `historical` / `incremental`: Bronze → Silver → database; `--skip-fetch` only loads pending files; `status`: watermarks, last runs, row counts |

## Start the database

```bash
docker compose up -d project-db            # or the whole stack: docker compose up -d
docker compose ps project-db               # wait for "healthy"
docker compose exec project-db psql -U volcano -d volcano -c "\dt"
```

If it does not start:

| Symptom | Cause / fix |
|---|---|
| `docker compose up -d` does not create `project-db` | Old copy of `docker-compose.yml` with `profiles: ["db"]`. Pull the latest branch |
| `port is already allocated` / `address already in use` on 5432 | A local PostgreSQL (or another container) uses 5432. Set `POSTGRES_PORT=5433` in `.env`, `docker compose up -d project-db`, and use 5433 in DBeaver |
| DBeaver: `password authentication failed` | The password was fixed when the volume was created. If `.env` changed afterwards, reset the volume (below) |
| Tables are missing / old columns | `db-migrate` did not run (e.g. only `project-db` was started). Run `docker compose up -d db-migrate` and read `docker compose logs db-migrate` |

## Connect with pgAdmin

1. *Servers → Register → Server…*; *General* tab: Name `Volcano (Docker)`.
2. *Connection* tab: Host `localhost`, Port `POSTGRES_PORT` from `.env`, Maintenance database `volcano`, Username `volcano`, Password = `POSTGRES_PASSWORD` from `.env`, tick *Save password* → *Save*.
3. `volcano → Schemas → public → Tables` (right click → *View/Edit Data*), or *Tools → Query Tool* for SQL. The `geom` columns open on a map with the *Geometry Viewer* (eye icon in the result grid).

If pgAdmin was installed together with a local PostgreSQL, that server already uses port 5432: set `POSTGRES_PORT=5433` in `.env` before the first `docker compose up -d project-db`.

## Connect with DBeaver

1. *Database → New Database Connection → PostgreSQL* → Next.
2. *Main* tab: Host `localhost`, Port `5432` (or your `POSTGRES_PORT`), Database `volcano`, Authentication *Database Native*, Username `volcano`, Password = `POSTGRES_PASSWORD` from `.env`. Tick *Save password*.
3. *Download* the driver files if DBeaver asks, then *Test Connection* → Finish.
4. Open `volcano → Schemas → public → Tables`. The PostGIS `geography` columns (`geom`) can be viewed on a map: open the table, *Data* tab, select the `geom` column → *Panels → Value viewer* / spatial view.
5. Quick check in an SQL editor (Ctrl+]):

```sql
SELECT * FROM source;                       -- 7 rows from 002
SELECT * FROM silver_quality_indicators;    -- all zeros until the loader runs
SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'earthquake';
```

Column comments (Silver meaning of each flag) appear in DBeaver under *Properties → Columns → Comment*.

## Apply schema changes

The `db-migrate` service runs on every `docker compose up -d`: it applies, in name order, each `sql/*.sql` that is new or whose content changed since it was applied (SHA-256 in the table `schema_migration`), each file in one transaction with its `schema_migration` row. A pulled SQL file therefore reaches every clone without manual steps.

Rules for SQL files:

* Add a new numbered file (`005_...sql`) for a change; edit an existing file only to fix it (it will be applied again).
* Every file must be **idempotent**: `CREATE TABLE IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS`, `CREATE OR REPLACE VIEW`, `INSERT ... ON CONFLICT DO NOTHING`. A database created before `db-migrate` existed gets every file once more.

```bash
docker compose up -d db-migrate                    # apply now, e.g. right after a git pull
docker compose logs db-migrate                     # "N file(s) applied, database volcano is up to date"
docker compose exec project-db psql -U volcano -d volcano -c "SELECT * FROM schema_migration"
```

Recreate the database from zero (deletes its data, not the Airflow metadata):

```bash
docker compose rm -sf project-db
docker volume rm volcano_project_db_data
docker compose up -d project-db
```

## Silver indicators in the database (002)

| Silver topic → table | Columns added | Meaning |
|---|---|---|
| all loaded rows → `event`, `volcano`, `volcano_update` | `bronze_file`, `bronze_sha256`, `silver_processed_utc` | Lineage: the Bronze JSON each row came from (Silver `processed_utc` → `silver_processed_utc`) |
| quarantine → `rejected_record` | `bronze_file`, `silver_processed_utc` | Same lineage for rejected records |
| `earthquakes` → `earthquake` | `nearest_vnum`, `distance_km`, `mag_missing`, `is_outlier_mag_iqr`, `is_outlier_mag_z`, `is_outlier_depth_iqr`, `is_outlier_depth_z`, `is_outlier_any` (generated) | Spatial link and outlier flags (IQR 1.5 and modified z-score 3.5). Outliers are flagged, never dropped |
| `eruptions` → `eruption` | `vei_missing` | GVP has no VEI (37 % of the sample) |
| `alert_notices` → `volcano_alert` | `alert_level_ord`, `coords_missing` | Ordinal level 1–4; HANS sent no coordinates |
| `volcano_updates` → **new** `volcano_update` | all Silver columns incl. `text_clean`, `tokens text[]`, `token_count`, `kw_*` | Scraped notices with NLP features. Own table, not a child of `event`, because most rows have no `vnum` |

View `silver_quality_indicators`: one row per (table, indicator) with `flagged`, `total`, `pct_flagged`, plus the quarantine count per reason.

## How the loader works

Entry points: the `load_to_database` task of the `silver_processing` DAG (mode `incremental`) and `python ingest.py --mode historical|incremental` (add `--skip-fetch` to only load files already on disk).

* Silver files are loaded oldest first; for the same timestamp `volcanoes` goes first, then `eruptions`, `earthquakes`, `alert_notices`, `volcano_updates` and the quarantine file.
* **One transaction per Silver file**: staging with `COPY` → upsert into `event` + domain table → `ingestion_run` (one row per source in the file, with rows in / new / updated / rejected) → `ingestion_state.watermark` → row in `silver_load_file`. A crash rolls back everything for that file, a `failed` run with the error is recorded and the loader stops; the next call retries the same file.
* **Idempotency**: a file listed in `silver_load_file` is never loaded again, and an existing row is rewritten only when its `content_hash` changed (`ON CONFLICT ... WHERE content_hash IS DISTINCT FROM ...`). Loading the same records again under a new file name gives 0 new / 0 updated.
* An advisory lock lets only one loader run at a time (Airflow task and `ingest.py` at the same time are safe).
* `event.source_id` comes from the Bronze file name of each row (`usgs_fdsn` or `usgs_feed` for earthquakes).
* Watermarks: `usgs_feed` / `usgs_fdsn` = latest USGS `updated`, `usgs_hans` = latest `sent_time`, `usgs_volcano_updates` = latest `as_of_time` (fixed-width UTC strings, they only move forward), GVP = `database_version` from `config/sources.yaml`.

Cases found when the Silver samples were first loaded by hand (2026-11-16) and how the loader handles them:

* HANS volcanoes that are not in the GVP list (Korovin, Coso, Long Valley, Yellowstone) are inserted from `usgs_hans.extra_volcanoes` in `config/sources.yaml` (`is_holocene = false`); their notices take the coordinates from there. A notice for any other unknown volcano goes to `rejected_record` with reason `volcano_not_in_catalog`, as do eruptions of volcanoes outside the catalogue.
* `volcano_update.volcano_number`: Silver `vnum` when known, otherwise an exact case-insensitive name match (only if the name is unique). 6 of 9 records are linked.
* `color_code` values outside GREEN/YELLOW/ORANGE/RED are stored as NULL.
* Quarantine payloads that are not JSON are stored as a JSON string.
* Eruptions: `start_month` / `start_day` are filled from `start_time` according to `time_precision`; BCE eruptions keep `event_time = NULL` and `time_precision = 'bce'`.
* `volcano_earthquake` gets one link per earthquake: the nearest volcano within the radius (Silver `nearest_vnum`).

Tests: [`tests/test_db_loader.py`](../tests/test_db_loader.py) checks the row preparation on the recorded Silver samples, without a database. The end-to-end test (load twice, no duplicates) runs only when `VOLCANO_TEST_DSN` points to a disposable database with every `sql/*.sql` applied:

```bash
docker compose exec project-db psql -U volcano -d volcano -c "CREATE DATABASE volcano_test"
docker compose run --rm -e PGDATABASE=volcano_test db-migrate
VOLCANO_TEST_DSN="host=localhost port=5433 dbname=volcano_test user=volcano password=..." pytest tests/test_db_loader.py
```

## Still open

* `volcano_earthquake` only stores the nearest volcano; all volcanoes within the radius (many-to-many) need a PostGIS query (`ST_DWithin`) after the load.
* `event_group` (cross-source matching) is empty: Workshop 3.
* `--mode incremental` still downloads the full 7-day feed; using the watermark in the FDSN query (`updatedafter=<watermark>`) is Workshop 3.
* The ER diagram should be regenerated with the 002/003 columns, `volcano_update`, `silver_load_file` and `schema_migration`.
