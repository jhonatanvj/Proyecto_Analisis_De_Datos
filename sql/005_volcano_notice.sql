-- Workshop 3: USGS HANS notice texts (Silver topic notices, source usgs_hans_notices).
-- Idempotent, applied by the db-migrate service.
--
-- Own table, not a child of event: 6 % of the notices are region-wide (e.g. "Cascade Range
-- volcanoes") and have no volcano and therefore no coordinates, which event requires. A notice is
-- linked to a volcano through volcano_number when its vnum is in the volcano table.

CREATE TABLE IF NOT EXISTS volcano_notice (
    record_id            text PRIMARY KEY,         -- 'usgs_hans_notice:<notice identifier>:<notice type>'
    content_hash         char(64) NOT NULL,
    source_id            text NOT NULL DEFAULT 'usgs_hans_notices' REFERENCES source,
    notice_identifier    text NOT NULL,            -- e.g. 'DOI-USGS-HVO-2026-11-18T18:08:44+00:00'
    notice_type          text NOT NULL,            -- DU daily, WU weekly, SR status, VAN, VV (VONA), IS, MU, BU
    sent_time            timestamptz NOT NULL,
    observatory          text,                     -- avo, hvo, cvo, calvo, yvo, nmi
    volcano_codes        text,                     -- USGS volcano codes as sent (comma separated)
    vnums                integer[] NOT NULL DEFAULT '{}', -- every volcano the notice covers
    volcano_number       integer REFERENCES volcano, -- first volcano of the notice, if in the catalogue
    volcano_name         text,
    region               text,                     -- HANS region, e.g. 'Alaska - Aleutians'
    is_region_wide       boolean NOT NULL DEFAULT false,
    alert_level          text,                     -- read from the text; highest level for multi-volcano notices
    alert_level_ord      smallint CHECK (alert_level_ord BETWEEN 1 AND 4),
    color_code           text,
    summary_clean        text,                     -- observatory's own summary paragraph, cleaned
    summary_tokens       text[] NOT NULL DEFAULT '{}',
    text_clean           text,                     -- body without header, footer and fixed sections
    tokens               text[] NOT NULL DEFAULT '{}',
    token_count          integer NOT NULL DEFAULT 0 CHECK (token_count >= 0),
    text_length          integer,                  -- characters of the body before cleaning
    summary_length       integer,
    notice_url           text,
    kw_eruption          boolean NOT NULL DEFAULT false,
    kw_ash               boolean NOT NULL DEFAULT false,
    kw_lava              boolean NOT NULL DEFAULT false,
    kw_seismicity        boolean NOT NULL DEFAULT false,
    kw_deformation       boolean NOT NULL DEFAULT false,
    kw_gas               boolean NOT NULL DEFAULT false,
    kw_plume             boolean NOT NULL DEFAULT false,
    bronze_file          text,
    bronze_sha256        char(64),
    silver_processed_utc timestamptz,
    updated_at           timestamptz NOT NULL DEFAULT now(),
    UNIQUE (notice_identifier, notice_type)
);
CREATE INDEX IF NOT EXISTS volcano_notice_vnum_idx ON volcano_notice (volcano_number, sent_time DESC);
CREATE INDEX IF NOT EXISTS volcano_notice_time_idx ON volcano_notice (sent_time);

COMMENT ON TABLE volcano_notice IS 'USGS HANS notice texts since 2010 (Workshop 3 text corpus)';
COMMENT ON COLUMN volcano_notice.summary_clean IS
    'Summary / Activity Summary / Activity Update / RMK section, or the first narrative paragraph';
COMMENT ON COLUMN volcano_notice.tokens IS 'Lowercase, no stop words (English + config/stopwords_domain.txt), diacritics folded';
