-- First draft of the schema (Workshop 1). The final version comes with Workshop 2.
-- PostgreSQL 16 + PostGIS. Times are stored in UTC and coordinates in WGS84 (EPSG:4326).
-- Scope: US volcanoes (GVP list filtered by country, plus the volcanoes in USGS HANS).
CREATE EXTENSION IF NOT EXISTS postgis;

-- Tables shared by every team (course data model)
CREATE TABLE source (
    source_id     text PRIMARY KEY,               -- e.g. 'usgs_feed'
    name          text NOT NULL,
    url           text NOT NULL,
    format        text NOT NULL,                  -- geojson | csv | json | rss_xml
    license       text NOT NULL,
    citation      text,
    role          text CHECK (role IN ('historical','incremental','optional'))
);

CREATE TABLE ingestion_run (
    run_id        bigserial PRIMARY KEY,
    mode          text NOT NULL CHECK (mode IN ('historical','incremental')),
    source_id     text REFERENCES source,
    started       timestamptz NOT NULL DEFAULT now(),
    finished      timestamptz,
    status        text NOT NULL CHECK (status IN ('running','success','failed','partial')),
    rows_in       integer DEFAULT 0,
    rows_new      integer DEFAULT 0,
    rows_updated  integer DEFAULT 0,
    rows_rejected integer DEFAULT 0,
    raw_file      text,                           -- landing-zone file processed
    error_message text
);

CREATE TABLE ingestion_state (
    source_id     text PRIMARY KEY REFERENCES source,
    watermark     text NOT NULL,                  -- typed per source (epoch ms, ISO date, db version)
    last_success  timestamptz,
    last_etag     text,
    last_modified text,
    last_run_id   bigint REFERENCES ingestion_run
);

CREATE TABLE event (
    event_id        text PRIMARY KEY,             -- '<namespace>:<source native id>'
    source_id       text NOT NULL REFERENCES source,
    event_type      text NOT NULL CHECK (event_type IN ('eruption','earthquake','alert_notice','activity_report')),
    event_time      timestamptz,                  -- NULL allowed for undated/BCE eruptions
    time_precision  text NOT NULL DEFAULT 'second', -- second|day|month|year|unknown
    latitude        double precision NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude       double precision NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    geom            geography(Point, 4326) GENERATED ALWAYS AS
                    (ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography) STORED,
    severity        double precision,
    severity_unit   text,                         -- 'VEI', 'magnitude (ml)', 'alert level 1-4', 'report_status'
    raw_reference   text NOT NULL,                -- original id / URL
    content_hash    char(64) NOT NULL,            -- SHA-256 of the source record (change detection)
    first_seen_run  bigint REFERENCES ingestion_run,
    last_seen_run   bigint REFERENCES ingestion_run,
    updated_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX event_time_idx ON event (event_type, event_time);
CREATE INDEX event_geom_idx ON event USING gist (geom);

CREATE TABLE event_group (
    group_id      text NOT NULL,                  -- e.g. 'vnum:351020:2026-W44'
    event_id      text NOT NULL REFERENCES event,
    rule          text NOT NULL,                  -- matching rule that produced the link
    score         double precision,
    PRIMARY KEY (group_id, event_id)
);

CREATE TABLE rejected_record (
    rejected_id   bigserial PRIMARY KEY,
    run_id        bigint REFERENCES ingestion_run,
    source_id     text REFERENCES source,
    reason        text NOT NULL,                  -- e.g. 'invalid_latitude', 'missing_time'
    payload       jsonb NOT NULL,
    rejected_at   timestamptz NOT NULL DEFAULT now()
);

-- Volcano-specific tables
-- Static geographic entity: one row per Holocene volcano (GVP)
CREATE TABLE volcano (
    volcano_number      integer PRIMARY KEY,      -- GVP vnum (6 digits)
    name                text NOT NULL,
    country             text,
    region              text,
    subregion           text,
    primary_type        text,
    tectonic_setting    text,
    elevation_m         integer,
    last_eruption_year  integer,                  -- negative = BCE; NULL = unknown
    evidence_category   text,
    latitude            double precision NOT NULL,
    longitude           double precision NOT NULL,
    geom                geography(Point, 4326) GENERATED ALWAYS AS
                        (ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography) STORED,
    is_holocene         boolean NOT NULL DEFAULT true, -- false: only in HANS (e.g. Yellowstone 325010)
    observatory         text,                     -- HANS obs_abbr: avo, hvo, cvo, calvo, yvo, nmi
    nvews_threat        text,                     -- HANS NVEWS threat class ('Very High Threat', ...)
    source_id           text NOT NULL REFERENCES source,  -- 'gvp_volcanoes' or 'usgs_hans'
    gvp_db_version      text,                     -- NULL for volcanoes that come only from HANS
    content_hash        char(64) NOT NULL
);
CREATE INDEX volcano_geom_idx ON volcano USING gist (geom);

CREATE TABLE eruption (
    event_id            text PRIMARY KEY REFERENCES event,
    eruption_number     integer UNIQUE NOT NULL,
    volcano_number      integer NOT NULL REFERENCES volcano,
    activity_type       text,                     -- 'Confirmed Eruption', 'Uncertain Eruption', ...
    vei                 smallint CHECK (vei BETWEEN 0 AND 8),
    vei_modifier        text,
    start_year          integer, start_month smallint, start_day smallint,
    start_uncertainty_days integer,
    end_year            integer, end_month smallint, end_day smallint,
    is_ongoing          boolean,
    evidence_method     text
);

CREATE TABLE earthquake (
    event_id            text PRIMARY KEY REFERENCES event,
    usgs_id             text UNIQUE NOT NULL,
    magnitude           double precision,
    mag_type            text,
    depth_km            double precision,
    place               text,
    review_status       text,                     -- automatic | reviewed | deleted
    network             text,
    source_updated      timestamptz               -- USGS 'updated' (watermark field)
);

-- Link earthquake -> nearby volcano(es) within the configured radius
CREATE TABLE volcano_earthquake (
    volcano_number      integer REFERENCES volcano,
    event_id            text REFERENCES earthquake,
    distance_km         double precision NOT NULL,
    PRIMARY KEY (volcano_number, event_id)
);

-- Official USGS alert notices (HANS): one row per (volcano, notice)
CREATE TABLE volcano_alert (
    event_id            text PRIMARY KEY REFERENCES event,   -- 'usgs_hans:<vnum>:<notice_identifier>'
    volcano_number      integer NOT NULL REFERENCES volcano,
    notice_identifier   text NOT NULL,            -- e.g. 'DOI-USGS-AVO-2026-11-09T20:51:49+00:00'
    alert_level         text NOT NULL CHECK (alert_level IN ('NORMAL','ADVISORY','WATCH','WARNING')),
    color_code          text CHECK (color_code IN ('GREEN','YELLOW','ORANGE','RED')),
    previous_alert_level text,                    -- level before this notice (change detection)
    previous_color_code text,
    notice_type         text,                     -- HANS notice_type_cd (DU, IS, VAN, ...)
    observatory         text,
    sent_utc            timestamptz NOT NULL,     -- from sent_unixtime
    notice_url          text,
    UNIQUE (volcano_number, notice_identifier)
);
CREATE INDEX volcano_alert_latest_idx ON volcano_alert (volcano_number, sent_utc DESC);

-- Current state per volcano (also covers volcanoes that never had a notice: NORMAL/GREEN by default)
CREATE VIEW volcano_current_alert AS
SELECT DISTINCT ON (v.volcano_number)
       v.volcano_number, v.name,
       coalesce(a.alert_level, 'NORMAL') AS alert_level,
       coalesce(a.color_code, 'GREEN')   AS color_code,
       a.sent_utc AS alert_since, a.notice_url
FROM volcano v
LEFT JOIN volcano_alert a USING (volcano_number)
ORDER BY v.volcano_number, a.sent_utc DESC NULLS LAST;

-- Optional source: Smithsonian / USGS Weekly Volcanic Activity Report
CREATE TABLE activity_report (
    event_id            text PRIMARY KEY REFERENCES event,
    volcano_number      integer REFERENCES volcano, -- NULL until linked by name (quarantine otherwise)
    volcano_name_raw    text NOT NULL,
    country_raw         text,
    week_start          date NOT NULL,
    week_end            date NOT NULL,
    report_status       text CHECK (report_status IN ('NEW','ONGOING')),
    report_text         text,
    pub_date            timestamptz,
    UNIQUE (volcano_name_raw, week_start)
);
