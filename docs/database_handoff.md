# Project database: current state and next steps

Status on 2026-11-16: **schema ready, not loaded yet**. The `project-db` container starts with the rest of the stack and creates the schema with the Silver quality indicators. The pipeline (Bronze JSON → Silver Parquet) does not write to the database yet. This note is for the team members who will build the database layer.

## Why we need it

The course project document (final delivery, 30%) asks for:

* PostgreSQL + PostGIS with the common tables `source`, `ingestion_run`, `ingestion_state`, `event`, `event_group` and `rejected_record`, plus volcano domain tables.
* `python ingest.py --mode historical | incremental | status`, idempotent, with a watermark per source that advances in the same transaction as the data.
* A dashboard on `localhost:8050` that reads only from the database or its views.

The Workshop 3 document asks for a Gold layer of Parquet files built with PySpark. The plan is to keep both: Silver Parquet → Gold Parquet for Spark, and Silver Parquet → database for `ingest.py` and the dashboard.

## What exists

| Piece | Where | State |
|---|---|---|
| Base schema (DDL) | [`sql/001_schema_draft.sql`](../sql/001_schema_draft.sql) | Workshop 1: 12 tables + view `volcano_current_alert` |
| Silver indicators (DDL) | [`sql/002_silver_indicators.sql`](../sql/002_silver_indicators.sql) | Workshop 2: quality flags, lineage, `volcano_update`, view `silver_quality_indicators`, `source` rows. Idempotent |
| ER diagram | [`docs/er_diagram.png`](er_diagram.png) (source `er_diagram.mmd`) | Matches 001; does not show the 002 columns yet |
| Mapping source field → table/column | [`workshop_1/mapping.md`](../workshop_1/mapping.md), [`mapping.csv`](../workshop_1/mapping.csv) | Workshop 1 |
| Ingestion contract (upserts, watermarks, retries) | [`workshop_1/ingestion_contract.md`](../workshop_1/ingestion_contract.md) | Workshop 1 |
| PostGIS container | service `project-db` in [`docker-compose.yml`](../docker-compose.yml) | Starts with `docker compose up -d`; runs every `.sql` in `sql/` (name order) on first start |
| Connection settings | `POSTGRES_*` in `.env` (copy of `.env.example`) | Host `localhost`, port 5432, db/user `volcano` |
| Clean data to load | `datalake_silver/<topic>_*.parquet` (topics `volcanoes`, `eruptions`, `earthquakes`, `alert_notices`, `volcano_updates`) and `datalake_silver/quarantine/` | Running every hour |
| CLI | [`ingest.py`](../ingest.py) | Stub: parses arguments, `--mode status` lists sources only |

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
| Tables are missing / old columns | The `.sql` files only run on an **empty** volume. Apply `002` by hand or reset the volume (below) |

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

Apply only `002` to an existing database (keeps data):

```bash
docker compose exec -T project-db psql -U volcano -d volcano < sql/002_silver_indicators.sql
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

Tested on 2026-11-16: the Silver samples in `workshop_2/samples/` (229 earthquakes, 1,281 eruptions, 54 notices, 9 updates, 146 rejects) load into this schema inside a transaction without errors. Results of that test, useful for the loader:

* `event.latitude/longitude` are `NOT NULL`. 4 HANS notices have `coords_missing = true` and belong to volcanoes that are not in the GVP list (e.g. Yellowstone 325010). The loader must first insert those volcanoes from HANS (`is_holocene = false`, coordinates from `getUSVolcanoes`) and copy their coordinates into `event`.
* Eruptions of non-US volcanoes must be skipped (FK to `volcano`).
* 6 of 9 `volcano_update` rows have no `vnum` (the "elevated" list only has the name). Link them by name to `volcano.name` on load or leave `volcano_number` NULL.
* `rejected_record.payload` is a JSON string in Parquet: cast with `::jsonb`.
* `volcano_alert.color_code` only accepts GREEN/YELLOW/ORANGE/RED (the only values seen in HANS so far). If HANS ever sends another value, the insert fails: map unknown values to NULL in the loader.

## Gaps still open

* The `eruption` table stores `start_year/month/day`; Silver has `start_time` + `time_precision` → use `event.event_time` and `event.time_precision`.
* FDSN (historical earthquakes since 2010) is loaded into Bronze by the manual DAG `bronze_backfill_fdsn` (or `scripts/run_pipeline_local.py --fdsn-backfill`) and ends in the Silver `earthquakes` topic like the feed. `--mode historical` can trigger that backfill and then load Silver into the database. The daily catch-up of revised events (`updatedafter=<watermark>`) is still to do (Workshop 3).
* The ER diagram should be regenerated with the 002 columns and `volcano_update`.

## Suggested next steps

1. Add `psycopg[binary]` and `SQLAlchemy` (already listed, commented, in `requirements.txt`) to `requirements.txt` and `airflow/requirements.txt`.
2. Write a loader `src/volcano_pipeline/storage/db.py`:
   - read Silver topic files not yet loaded;
   - upsert into `event` + the domain table with `INSERT ... ON CONFLICT (event_id) DO UPDATE ... WHERE content_hash <> EXCLUDED.content_hash`, copying the indicator and lineage columns 1:1;
   - write one `ingestion_run` row per run and source;
   - advance `ingestion_state.watermark` in the same transaction.
3. Call it from `ingest.py` (`--mode historical`, `--mode incremental`, `--mode status`) and as a last task in the Silver DAG. For the DAG, add `POSTGRES_HOST=project-db` to the Airflow environment (inside the Docker network the host is the service name, not `localhost`).
4. Tests with pytest. Use a recorded Silver Parquet as fixture. For SQL, either a disposable PostGIS container or keep the SQL in small functions that can be tested without a database.
