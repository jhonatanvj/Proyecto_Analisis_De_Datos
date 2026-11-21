-- Workshop 2: Silver quality indicators and lineage in the project database.
-- Applied by the db-migrate service after 001. Idempotent (IF NOT EXISTS / OR REPLACE / ON CONFLICT).
--
-- Column names follow the Silver Parquet schemas (src/volcano_pipeline/silver/schemas.py) so the
-- loader can copy them 1:1. Silver record_id = event_id ('<namespace>:<native id>').
-- Silver lineage columns map as: bronze_file -> bronze_file, bronze_sha256 -> bronze_sha256,
-- processed_utc -> silver_processed_utc.

-- 1. Sources (catalogue from config/sources.yaml), needed by every FK to source ------------------
INSERT INTO source (source_id, name, url, format, license, citation, role) VALUES
 ('gvp_volcanoes', 'Smithsonian GVP - Volcanoes of the World - Holocene Volcanoes',
  'https://webservices.volcano.si.edu/geoserver/GVP-VOTW/ows', 'geojson',
  'Smithsonian GVP terms of use; cite database version and DOI',
  'Global Volcanism Program, 2026. Volcanoes of the World (v. 5.4.0). https://doi.org/10.5479/si.GVP.VOTW5-2026.5.4',
  'historical'),
 ('gvp_eruptions', 'Smithsonian GVP - Volcanoes of the World - Holocene Eruptions',
  'https://webservices.volcano.si.edu/geoserver/GVP-VOTW/ows', 'geojson',
  'Smithsonian GVP terms of use; cite database version and DOI',
  'Global Volcanism Program, 2026. Volcanoes of the World (v. 5.4.0). https://doi.org/10.5479/si.GVP.VOTW5-2026.5.4',
  'historical'),
 ('usgs_fdsn', 'USGS FDSN Event Web Service (ComCat)',
  'https://earthquake.usgs.gov/fdsnws/event/1/query', 'csv',
  'Public domain (U.S. Government). Credit: U.S. Geological Survey',
  'U.S. Geological Survey, Earthquake Hazards Program, ANSS Comprehensive Catalog (ComCat)',
  'historical'),
 ('usgs_hans', 'USGS HANS public API - volcano alert levels and aviation colour codes',
  'https://volcanoes.usgs.gov/hans-public/api', 'json',
  'Public domain (U.S. Government). Credit: U.S. Geological Survey Volcano Hazards Program',
  'U.S. Geological Survey, Volcano Hazards Program, Hazard Notification System (HANS) public API',
  'incremental'),
 ('usgs_feed', 'USGS real-time GeoJSON summary feed (all earthquakes, past 7 days)',
  'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_week.geojson', 'geojson',
  'Public domain (U.S. Government). Credit: U.S. Geological Survey',
  'U.S. Geological Survey, Earthquake Hazards Program, real-time GeoJSON feeds',
  'incremental'),
 ('usgs_volcano_updates', 'USGS Volcano Hazards Program - Volcano Updates page (web scraping)',
  'https://www.usgs.gov/programs/VHP/volcano-updates', 'html',
  'Public domain (U.S. Government). Credit: U.S. Geological Survey Volcano Hazards Program',
  'U.S. Geological Survey, Volcano Hazards Program, Volcano Updates',
  'incremental'),
 ('gvp_weekly_rss', 'Smithsonian / USGS Weekly Volcanic Activity Report (RSS)',
  'https://volcano.si.edu/news/WeeklyVolcanoRSS.xml', 'rss_xml',
  'Smithsonian GVP terms of use; cite the Weekly Volcanic Activity Report',
  'Global Volcanism Program, Smithsonian / USGS Weekly Volcanic Activity Report, Sennert, S. (ed.)',
  'optional')
ON CONFLICT (source_id) DO NOTHING;

-- 2. Lineage: every row loaded from Silver points back to its Bronze file -----------------------
ALTER TABLE event
    ADD COLUMN IF NOT EXISTS bronze_file          text,
    ADD COLUMN IF NOT EXISTS bronze_sha256        char(64),
    ADD COLUMN IF NOT EXISTS silver_processed_utc timestamptz;

ALTER TABLE volcano
    ADD COLUMN IF NOT EXISTS bronze_file          text,
    ADD COLUMN IF NOT EXISTS bronze_sha256        char(64),
    ADD COLUMN IF NOT EXISTS silver_processed_utc timestamptz;

-- Silver quarantine (datalake_silver/quarantine/rejected_*.parquet) keeps the Bronze file too.
-- payload is a JSON string in Parquet: cast with ::jsonb on load.
ALTER TABLE rejected_record
    ADD COLUMN IF NOT EXISTS bronze_file          text,
    ADD COLUMN IF NOT EXISTS silver_processed_utc timestamptz;

-- 3. Earthquakes: missing-value and outlier flags (flagged, never dropped) ---------------------
ALTER TABLE earthquake
    ADD COLUMN IF NOT EXISTS nearest_vnum         integer REFERENCES volcano,
    ADD COLUMN IF NOT EXISTS distance_km          double precision CHECK (distance_km >= 0),
    ADD COLUMN IF NOT EXISTS mag_missing          boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS is_outlier_mag_iqr   boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS is_outlier_mag_z     boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS is_outlier_depth_iqr boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS is_outlier_depth_z   boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS is_outlier_any       boolean GENERATED ALWAYS AS
        (is_outlier_mag_iqr OR is_outlier_mag_z OR is_outlier_depth_iqr OR is_outlier_depth_z) STORED;

COMMENT ON COLUMN earthquake.nearest_vnum IS 'Silver: closest US volcano (GVP vnum) within spatial.volcano_radius_km';
COMMENT ON COLUMN earthquake.distance_km IS 'Silver: haversine distance to nearest_vnum, km';
COMMENT ON COLUMN earthquake.mag_missing IS 'Silver: magnitude was null in the source (kept, not imputed)';
COMMENT ON COLUMN earthquake.is_outlier_mag_iqr IS 'Silver: magnitude outside Q1-1.5*IQR .. Q3+1.5*IQR of the batch';
COMMENT ON COLUMN earthquake.is_outlier_mag_z IS 'Silver: |modified z-score| of magnitude > 3.5 (Iglewicz-Hoaglin, MAD)';
COMMENT ON COLUMN earthquake.is_outlier_depth_iqr IS 'Silver: depth outside the IQR fences of the batch';
COMMENT ON COLUMN earthquake.is_outlier_depth_z IS 'Silver: |modified z-score| of depth > 3.5';
COMMENT ON COLUMN earthquake.is_outlier_any IS 'Generated: any of the four outlier flags';

-- 4. Eruptions: VEI not assigned by GVP ----------------------------------------------------------
ALTER TABLE eruption
    ADD COLUMN IF NOT EXISTS vei_missing boolean NOT NULL DEFAULT false;
COMMENT ON COLUMN eruption.vei_missing IS 'Silver: GVP has no VEI for this eruption (vei is NULL, not imputed)';
COMMENT ON COLUMN event.time_precision IS 'Silver time_precision: second|day|month|year|unknown (GVP partial dates)';

-- 5. HANS alert notices: ordinal level and missing coordinates ----------------------------------
ALTER TABLE volcano_alert
    ADD COLUMN IF NOT EXISTS alert_level_ord smallint CHECK (alert_level_ord BETWEEN 1 AND 4),
    ADD COLUMN IF NOT EXISTS coords_missing  boolean NOT NULL DEFAULT false;
COMMENT ON COLUMN volcano_alert.alert_level_ord IS 'Silver: NORMAL=1, ADVISORY=2, WATCH=3, WARNING=4';
COMMENT ON COLUMN volcano_alert.coords_missing IS
    'Silver: HANS sent no coordinates; event.latitude/longitude were taken from the volcano table on load';

-- 6. Scraped USGS Volcano Updates (Silver topic volcano_updates) ---------------------------------
-- Own table, not a child of event: many records have no vnum and therefore no coordinates.
CREATE TABLE IF NOT EXISTS volcano_update (
    record_id            text PRIMARY KEY,         -- Silver record_id ('usgs_vu:<type>:<obs>:<name>:<key>')
    content_hash         char(64) NOT NULL,
    source_id            text NOT NULL DEFAULT 'usgs_volcano_updates' REFERENCES source,
    record_type          text NOT NULL CHECK (record_type IN ('elevated','observatory_notice')),
    observatory          text,
    volcano_name         text,
    volcano_number       integer REFERENCES volcano, -- NULL when the name did not match a US volcano
    alert_level          text,                     -- scraped text: no CHECK, a new wording must not break the load
    color_code           text,
    as_of_time           timestamptz,
    issued_raw           text,
    notice_number        text,
    notice_url           text,
    -- NLP text preprocessing (Silver text.py)
    text_clean           text,
    tokens               text[] NOT NULL DEFAULT '{}',
    token_count          integer NOT NULL DEFAULT 0 CHECK (token_count >= 0),
    kw_eruption          boolean NOT NULL DEFAULT false,
    kw_ash               boolean NOT NULL DEFAULT false,
    kw_lava              boolean NOT NULL DEFAULT false,
    kw_seismicity        boolean NOT NULL DEFAULT false,
    kw_deformation       boolean NOT NULL DEFAULT false,
    kw_gas               boolean NOT NULL DEFAULT false,
    kw_plume             boolean NOT NULL DEFAULT false,
    -- lineage
    bronze_file          text,
    bronze_sha256        char(64),
    silver_processed_utc timestamptz,
    updated_at           timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS volcano_update_vnum_idx ON volcano_update (volcano_number, as_of_time DESC);
COMMENT ON COLUMN volcano_update.tokens IS 'Silver: lowercased, punctuation and English stop words removed';
COMMENT ON COLUMN volcano_update.kw_eruption IS 'Silver: notice text mentions eruption terms (same for the other kw_* flags)';

-- 7. One view with every Silver indicator, to check data quality at a glance (DBeaver, dashboard)
CREATE OR REPLACE VIEW silver_quality_indicators AS
WITH eq AS (SELECT count(*) AS n,
                   count(*) FILTER (WHERE mag_missing)          AS mag_missing,
                   count(*) FILTER (WHERE is_outlier_mag_iqr)   AS mag_iqr,
                   count(*) FILTER (WHERE is_outlier_mag_z)     AS mag_z,
                   count(*) FILTER (WHERE is_outlier_depth_iqr) AS depth_iqr,
                   count(*) FILTER (WHERE is_outlier_depth_z)   AS depth_z,
                   count(*) FILTER (WHERE is_outlier_any)       AS any_outlier
            FROM earthquake),
     er AS (SELECT count(*) AS n, count(*) FILTER (WHERE vei_missing) AS vei_missing FROM eruption),
     al AS (SELECT count(*) AS n, count(*) FILTER (WHERE coords_missing) AS coords_missing FROM volcano_alert),
     vu AS (SELECT count(*) AS n, count(*) FILTER (WHERE volcano_number IS NULL) AS unmatched FROM volcano_update),
     rows (table_name, indicator, flagged, total) AS (
        SELECT 'earthquake', 'mag_missing',          mag_missing, n FROM eq UNION ALL
        SELECT 'earthquake', 'is_outlier_mag_iqr',   mag_iqr,     n FROM eq UNION ALL
        SELECT 'earthquake', 'is_outlier_mag_z',     mag_z,       n FROM eq UNION ALL
        SELECT 'earthquake', 'is_outlier_depth_iqr', depth_iqr,   n FROM eq UNION ALL
        SELECT 'earthquake', 'is_outlier_depth_z',   depth_z,     n FROM eq UNION ALL
        SELECT 'earthquake', 'is_outlier_any',       any_outlier, n FROM eq UNION ALL
        SELECT 'eruption',   'vei_missing',          vei_missing, n FROM er UNION ALL
        SELECT 'volcano_alert', 'coords_missing',    coords_missing, n FROM al UNION ALL
        SELECT 'volcano_update', 'volcano_unmatched', unmatched,  n FROM vu UNION ALL
        SELECT 'rejected_record', 'reason=' || reason, count(*), NULL FROM rejected_record GROUP BY reason)
SELECT table_name, indicator, flagged, total,
       round(100.0 * flagged / nullif(total, 0), 2) AS pct_flagged
FROM rows
ORDER BY table_name, indicator;
