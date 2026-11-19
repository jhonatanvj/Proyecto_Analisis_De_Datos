-- Loader bookkeeping: which Silver Parquet files are already in the database.
-- Runs after 002 on the first start of project-db. Idempotent, so it can also be applied by hand:
--   docker compose exec -T project-db psql -U volcano -d volcano < sql/003_loader_state.sql
--
-- The loader (src/volcano_pipeline/storage/loader.py) loads each Silver file in one transaction:
-- domain rows + ingestion_run + ingestion_state.watermark + one row here. A file listed here is
-- never loaded again, so running the loader twice creates no duplicates.

CREATE TABLE IF NOT EXISTS silver_load_file (
    file_name      text PRIMARY KEY,              -- path relative to datalake_silver/, e.g. 'quarantine/rejected_20261116_225940.parquet'
    topic          text NOT NULL,                 -- volcanoes | eruptions | earthquakes | alert_notices | volcano_updates | quarantine
    rows_in        integer NOT NULL DEFAULT 0,
    rows_new       integer NOT NULL DEFAULT 0,
    rows_updated   integer NOT NULL DEFAULT 0,
    rows_rejected  integer NOT NULL DEFAULT 0,
    run_ids        bigint[] NOT NULL DEFAULT '{}', -- ingestion_run rows written for this file (one per source)
    loaded_at      timestamptz NOT NULL DEFAULT now()
);

-- Name lookups when a scraped Volcano Update only carries the volcano name
CREATE INDEX IF NOT EXISTS volcano_name_lower_idx ON volcano (lower(name));
CREATE INDEX IF NOT EXISTS ingestion_run_source_idx ON ingestion_run (source_id, started DESC);
