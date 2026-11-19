-- activity_report is empty on purpose. Idempotent, applied by the db-migrate service.
--
-- The table was designed in Workshop 1 for the optional source gvp_weekly_rss (Smithsonian / USGS
-- Weekly Volcanic Activity Report). That source was never ingested: in Workshop 2 the weekly report
-- HTML answered 403 to scripted requests (bot challenge) and the RSS feed had not been updated since
-- 15 Oct 2026, with about two US items per week. It stays in config/sources.yaml as optional, so the
-- table is kept as its target instead of being dropped from the Workshop 1 data model.
-- The USGS texts actually used live in volcano_update (scraped Volcano Updates) and volcano_notice
-- (HANS notice archive).

COMMENT ON TABLE activity_report IS
    'Empty on purpose: target of the optional source gvp_weekly_rss (Weekly Volcanic Activity Report), '
    'never ingested (weekly report HTML blocks scripted requests, RSS stale since 2026-10-15). '
    'USGS texts in use: volcano_update and volcano_notice.';
