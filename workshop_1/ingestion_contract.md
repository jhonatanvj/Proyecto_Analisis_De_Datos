# Ingestion Contract Draft (Workshop #1)

General rules for all sources:

* Every response is first written **unmodified** to the raw landing zone (`data/raw/<source>_YYYYMMDD_HHMMSS.<ext>` + `.meta.json` with URL, headers and SHA-256). Processing reads from the landing zone, so any run can be re-processed without calling the API again.
* Writes are **upserts** on the duplicate key: `INSERT ... ON CONFLICT (event_id) DO UPDATE ... WHERE content_hash <> EXCLUDED.content_hash`. Re-running any mode produces no duplicates.
* One `ingestion_run` row per source and execution (`running` → `success`/`failed`), with `rows_in`, `rows_new`, `rows_updated`, `rows_rejected`.
* **The watermark advances only in the same transaction that commits the data** for that source. If a run fails halfway, the transaction is rolled back, the watermark keeps its old value, and the next run repeats the same interval. Upserts make that repetition harmless.
* HTTP: timeout (10 s connect / 120 s read), up to 4 retries with exponential backoff (2, 4, 8, 16 s) on 429/5xx, honoring `Retry-After`; a polite `User-Agent` with a contact; conditional requests (`If-None-Match` / `If-Modified-Since`) where the server sends ETag/Last-Modified.

| | **GVP Holocene volcanoes** | **GVP Holocene eruptions** | **USGS FDSN (ComCat)** | **Weekly Volcanic Activity Report** | **USGS GeoJSON feed** |
|---|---|---|---|---|---|
| Role | Historical (reference entity) | Historical | Historical | Incremental | Incremental |
| Format / access | WFS → GeoJSON | WFS → GeoJSON | REST → CSV | RSS 2.0 XML (CAP as backup) | GeoJSON |
| Historical strategy | One request, full layer (~1,400 features) | One request, full layer (~11,000 features, a few MB); no paging needed. Fallback: `maxFeatures` + `startIndex` | Radial query per volcano (`latitude`, `longitude`, `maxradiuskm=30`, `minmagnitude=2.5`) for volcanoes with last eruption ≥ 1900, **2010-01-01 → today**, partitioned by year if a `count` call returns > 20,000 | Not available as an API before the current week; the archive (since Nov 2000) is only HTML. Out of scope for bulk load; history of activity comes from GVP eruptions | Not historical (rolling 7-day window); the FDSN load covers the past |
| Incremental strategy | Monthly check of the database version; reload if it changes | Monthly; reload layer and compare `content_hash` per `Eruption_Number` (new/changed eruptions, e.g. end dates of ongoing eruptions) | Daily catch-up with `updatedafter=<watermark>` around selected volcanoes (catches revised magnitudes/locations and deletions flagged by status) | Poll every 6 h with ETag/Last-Modified; process items whose `(vnum, week_start)` is new or whose hash changed | Poll `all_week` every 15 min; process features with `properties.updated > watermark`; keep only those within 30 km of a volcano (others counted, not stored) |
| Watermark | `gvp_db_version` (e.g. `5.4.0`) | `gvp_db_version` + per-row hash | max(`updated`) committed (ISO UTC) | max(`week_end`) committed + last ETag | max(`properties.updated`) committed (epoch ms) |
| Overlap window | — | — | Re-query the last **7 days** on each run (late revisions) | Re-read the whole feed each poll (it only holds the current week) | Feed is a 7-day window: each poll naturally overlaps; the watermark filters |
| Duplicate key | `Volcano_Number` | `Eruption_Number` | `id` (→ `event_id = usgs:<id>`) | `(volcano_number, week_start)` | `id` (same namespace as FDSN) |
| Revisions / deletions | Yes: names, types, last eruption year change between versions | Yes: ongoing eruptions get end dates; VEI revised; eruptions can be discredited | Yes: magnitude, location and `status` (automatic → reviewed) change; events can be deleted | Text can be corrected after publication (detected by hash) | Yes: `updated` changes; deleted events disappear from the feed → mark as `deleted` after a FDSN check |
| Polling frequency | @monthly | @monthly | @daily | every 6 h (published weekly, Wednesdays ~23:00 UTC) | every 15 min |

Optional extensions (enabled only if time allows and approved): GDACS events API (`eventlist=VO`, watermark `datemodified`) and USGS HANS (alert levels and aviation colour codes for US volcanoes).
