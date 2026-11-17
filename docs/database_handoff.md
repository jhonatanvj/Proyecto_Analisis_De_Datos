# Project database: current state and next steps

Status on 2026-11-16: **draft**. The pipeline (Bronze JSON → Silver Parquet) does not write to a database yet. This note is for the team members who will build the database layer.

## Why we need it

The course project document (final delivery, 30%) asks for:

* PostgreSQL + PostGIS with the common tables `source`, `ingestion_run`, `ingestion_state`, `event`, `event_group` and `rejected_record`, plus volcano domain tables.
* `python ingest.py --mode historical | incremental | status`, idempotent, with a watermark per source that advances in the same transaction as the data.
* A dashboard on `localhost:8050` that reads only from the database or its views.

The Workshop 3 document asks for a Gold layer of Parquet files built with PySpark. The plan is to keep both: Silver Parquet → Gold Parquet for Spark, and Silver Parquet → database for `ingest.py` and the dashboard.

## What exists

| Piece | Where | State |
|---|---|---|
| Draft schema (DDL) | [`sql/001_schema_draft.sql`](../sql/001_schema_draft.sql) | Written in Workshop 1. Tested on 2026-11-16: it loads cleanly in PostGIS 16-3.4 (12 tables + view `volcano_current_alert`) |
| ER diagram | [`docs/er_diagram.png`](er_diagram.png) (source `er_diagram.mmd`) | Matches the draft |
| Mapping source field → table/column | [`workshop_1/mapping.md`](../workshop_1/mapping.md), [`mapping.csv`](../workshop_1/mapping.csv) | Workshop 1 |
| Ingestion contract (upserts, watermarks, retries) | [`workshop_1/ingestion_contract.md`](../workshop_1/ingestion_contract.md) | Workshop 1 |
| PostGIS container | service `project-db` in [`docker-compose.yml`](../docker-compose.yml), profile `db` | Starts only on request; runs every `.sql` in `sql/` on first start |
| Connection settings | `POSTGRES_*` in `.env.example` | Host `localhost`, port 5432, db/user `volcano` |
| Clean data to load | `datalake_silver/<topic>_*.parquet` (topics `volcanoes`, `eruptions`, `earthquakes`, `alert_notices`, `volcano_updates`) and `datalake_silver/quarantine/` | Running every hour |
| CLI | [`ingest.py`](../ingest.py) | Stub: parses arguments, `--mode status` lists sources only |

Start the draft database and look at it:

```bash
docker compose --profile db up -d project-db
docker compose --profile db exec project-db psql -U volcano -d volcano -c "\dt"
# from the host (any SQL client): localhost:5432, db volcano, user volcano, password from .env
```

The `.sql` files run only when the data volume is empty. After editing the schema, recreate it:

```bash
docker compose --profile db down project-db
docker volume rm proyectofinalanalisisdatos_project-db-data   # name = <folder>_project-db-data; check with: docker volume ls
docker compose --profile db up -d project-db
```

## Gaps between the draft and the current pipeline

* There is no table for the scraped USGS notices (`volcano_updates` topic). One option is a domain table `volcano_update` with `event_type = 'activity_report'`.
* The `earthquake` table has no column for the Silver outlier flags (`is_outlier_*`) or `mag_missing`.
* The `eruption` table stores `start_year/month/day`; Silver also has `start_time` + `time_precision` (as `event.time_precision` in the draft).
* `rejected_record.payload` is `jsonb`, but Silver quarantine stores it as a JSON string. Cast it on load.
* FDSN (historical earthquakes since 2010) is loaded into Bronze by the manual DAG `bronze_backfill_fdsn` (or `scripts/run_pipeline_local.py --fdsn-backfill`) and ends in the Silver `earthquakes` topic like the feed. `--mode historical` can trigger that backfill and then load Silver into the database. The daily catch-up of revised events (`updatedafter=<watermark>`) is still to do (Workshop 3).

## Suggested next steps

1. Rename the final DDL to `sql/002_schema.sql` (or edit the draft) and close the gaps above.
2. Add `psycopg[binary]` and `SQLAlchemy` (already listed, commented, in `requirements.txt`) to `requirements.txt` and `airflow/requirements.txt`.
3. Write a loader `src/volcano_pipeline/storage/db.py`:
   - read Silver topic files not yet loaded;
   - upsert into `event` + the domain table with `INSERT ... ON CONFLICT (event_id) DO UPDATE ... WHERE content_hash <> EXCLUDED.content_hash`;
   - write one `ingestion_run` row per run and source;
   - advance `ingestion_state.watermark` in the same transaction.
4. Call it from `ingest.py` (`--mode historical`, `--mode incremental`, `--mode status`) and as a last task in the Silver DAG. For the DAG, add the `project-db` service to the default profile and `POSTGRES_HOST=project-db` to the Airflow environment.
5. Tests with pytest. Use a recorded Silver Parquet as fixture. For SQL, either a disposable PostGIS container or keep the SQL in small functions that can be tested without a database.
