# Canonical Mapping Table (Workshop #1)

Scope: **United States volcanoes**. Every source field is mapped to the canonical `event` table (one row per source record) or to a domain table. Machine-readable version: [`mapping.csv`](mapping.csv).

## Normalization rules

| Aspect | Rule |
|---|---|
| Timestamps | Stored as `timestamptz` in **UTC**. USGS `time`/`updated` (epoch ms or ISO `Z`) are already UTC. GVP partial dates become the first day of the known period and `time_precision` records `day`/`month`/`year`; BCE eruptions keep `event_time = NULL` and are excluded from time-series analysis (kept in `eruption.start_year`). HANS notices use `sent_unixtime` (epoch seconds); `sent_utc` has no time-zone suffix and is display-only. Weekly reports use the week end date at 00:00 UTC. |
| Coordinates | WGS84 (EPSG:4326), decimal degrees; latitude in [-90, 90], longitude normalized to [-180, 180). Invalid or missing coordinates go to `rejected_record` (except reports, which inherit the volcano position). |
| Severity | Each event type keeps its own scale and `severity_unit`: eruptions → **VEI** (0–8, dimensionless, logarithmic in erupted tephra volume); earthquakes → **magnitude** with its `magType` (ml, md, mb, mww…; magnitudes of different types are *not* converted, only labeled); HANS notices → **USGS volcano alert level** ordinal (1 = NORMAL, 2 = ADVISORY, 3 = WATCH, 4 = WARNING; ground hazard), with the **aviation colour code** (GREEN/YELLOW/ORANGE/RED) kept as an attribute in `volcano_alert`; weekly reports → ordinal **report status** (1 = continuing, 2 = new activity). |
| Depth | km, positive downward (USGS convention). Negative depths (above sea level, common at volcanoes) are valid and kept. |
| Identifiers | `event_id = '<namespace>:<native id>'`. USGS FDSN and feed share the same `id` namespace (`usgs:`), so a feed record that is already in the historical load updates the same row. |
| Change detection | `content_hash` = SHA-256 of the source record (sorted keys). A different hash for an existing `event_id` = revised record → update + new `last_seen_run`. |
| Linking to volcano | Earthquakes are linked to every volcano within `volcano_radius_km` (30 km, configurable) in `volcano_earthquake`, with the great-circle distance. HANS notices and weekly reports are linked by `vnum` (HANS field; RSS `guid` `#vn_<vnum>`), never by name. Invalid vnums (`null`, `0`, `999999`) go to quarantine. |

## Field mapping

| Source | Source field | Target table | Target field | Transformation | Notes |
|---|---|---|---|---|---|
| gvp_eruptions | Eruption_Number | event | event_id | 'gvp_eruption:' \|\| Eruption_Number | stable GVP eruption id |
| gvp_eruptions | (constant) | event | source | 'gvp_eruptions' |  |
| gvp_eruptions | (constant) | event | event_type | 'eruption' |  |
| gvp_eruptions | StartDateYear/StartDateMonth/StartDateDay | event | timestamp | UTC date; month/day 0\|null -> 1; precision stored in time_precision; BCE (year<1) -> NULL | partial dates are the norm in GVP |
| gvp_eruptions | geometry.coordinates[1] | event | latitude | float, WGS84 | the layer has **no** Latitude property; volcano summit position (not vent) |
| gvp_eruptions | geometry.coordinates[0] | event | longitude | float, WGS84, normalized to [-180,180) | no Longitude property |
| gvp_eruptions | ExplosivityIndexMax | event | severity | int 0-8; null kept as NULL (VEI unknown) | severity_unit='VEI' |
| gvp_eruptions | Volcano_Number | event | raw_reference | 'https://volcano.si.edu/volcano.cfm?vn=' \|\| vnum | + content_hash = SHA-256(properties) |
| gvp_eruptions | Volcano_Number | eruption | volcano_number | int | FK -> volcano |
| gvp_eruptions | Activity_Type | eruption | activity_type | text | Confirmed / Uncertain / Discredited |
| gvp_eruptions | ExplosivityIndexModifier | eruption | vei_modifier | text |  |
| gvp_eruptions | EndDateYear/EndDateMonth/EndDateDay | eruption | end_* | int; null end = ongoing or unknown | is_ongoing derived |
| gvp_volcanoes | Volcano_Number | volcano | volcano_number | int | PK (vnum) |
| gvp_volcanoes | Volcano_Name | volcano | name | trim |  |
| gvp_volcanoes | Country | volcano | country | trim |  |
| gvp_volcanoes | Primary_Volcano_Type | volcano | primary_type | trim |  |
| gvp_volcanoes | Last_Eruption_Year | volcano | last_eruption_year | int; negative = BCE; 'Unknown' -> NULL |  |
| gvp_volcanoes | Elevation | volcano | elevation_m | int metres a.s.l. (negative = submarine) |  |
| gvp_volcanoes | Latitude/Longitude | volcano | latitude/longitude | float WGS84 |  |
| usgs_fdsn | id | event | event_id | 'usgs:' \|\| id | same id space as usgs_feed -> natural dedup key |
| usgs_fdsn | (constant) | event | source | 'usgs_fdsn' |  |
| usgs_fdsn | (constant) | event | event_type | 'earthquake' | only rows with type='earthquake'; others (quarry blast, explosion) -> rejected |
| usgs_fdsn | time | event | timestamp | ISO-8601 'Z' -> timestamptz UTC |  |
| usgs_fdsn | latitude | event | latitude | float |  |
| usgs_fdsn | longitude | event | longitude | float normalized |  |
| usgs_fdsn | mag | event | severity | float | severity_unit = 'magnitude (' \|\| magType \|\| ')' |
| usgs_fdsn | id | event | raw_reference | 'https://earthquake.usgs.gov/earthquakes/eventpage/' \|\| id | + content_hash |
| usgs_fdsn | depth | earthquake | depth_km | float km (can be negative = above sea level) |  |
| usgs_fdsn | magType | earthquake | mag_type | lower() | ml, md, mb, mww, ... |
| usgs_fdsn | status | earthquake | review_status | text | automatic \| reviewed |
| usgs_fdsn | updated | earthquake | source_updated | timestamptz UTC | incremental watermark |
| usgs_fdsn | place | earthquake | place | text |  |
| usgs_feed | id | event | event_id | 'usgs:' \|\| id |  |
| usgs_feed | properties.time | event | timestamp | epoch ms -> timestamptz UTC |  |
| usgs_feed | geometry.coordinates[1] | event | latitude | float |  |
| usgs_feed | geometry.coordinates[0] | event | longitude | float normalized |  |
| usgs_feed | geometry.coordinates[2] | earthquake | depth_km | float km |  |
| usgs_feed | properties.mag | event | severity | float | severity_unit from properties.magType |
| usgs_feed | properties.url | event | raw_reference | text | + content_hash |
| usgs_feed | properties.updated | earthquake | source_updated | epoch ms -> timestamptz | watermark |
| usgs_feed | properties.status | earthquake | review_status | text |  |
| usgs_feed | properties.type | (filter) | (filter) | keep 'earthquake' |  |
| gvp_weekly_rss | guid (`#vn_<vnum>`) + title week start | event | event_id | 'gvp_weekly:' \|\| vnum \|\| ':' \|\| week_start |  |
| gvp_weekly_rss | (constant) | event | source | 'gvp_weekly_rss' |  |
| gvp_weekly_rss | (constant) | event | event_type | 'activity_report' |  |
| gvp_weekly_rss | title (week range) | event | timestamp | week_end date 00:00 UTC (day precision) |  |
| gvp_weekly_rss | georss:point | event | latitude/longitude | split 'lat lon'; fallback: volcano table by vnum |  |
| gvp_weekly_rss | title suffix ('- New Eruptive Activity' / '- Continuing Eruptive Activity') | event | severity | New -> 2, otherwise 1 | severity_unit='report_status' |
| gvp_weekly_rss | link | event | raw_reference | text | + content_hash (text can be edited) |
| gvp_weekly_rss | title name / country | activity_report | volcano_name_raw / country_raw | parsed by regex |  |
| gvp_weekly_rss | guid vnum | activity_report | volcano_number | int; items of non-US volcanoes are skipped (scope) |  |
| gvp_weekly_rss | description | activity_report | report_text | HTML stripped |  |
| gvp_weekly_rss | pubDate | activity_report | pub_date | RFC-822 -> timestamptz UTC |  |
| usgs_hans | vnum + notice_identifier | event | event_id | 'usgs_hans:' \|\| vnum \|\| ':' \|\| notice_identifier | a notice can cover several volcanoes, so the key includes vnum |
| usgs_hans | (constant) | event | source | 'usgs_hans' |  |
| usgs_hans | (constant) | event | event_type | 'alert_notice' |  |
| usgs_hans | sent_unixtime | event | timestamp | epoch s -> timestamptz UTC | `0` = no notice ever issued -> not an event (state NORMAL/GREEN by default) |
| usgs_hans | vnum (lookup) | event | latitude/longitude | coordinates of the volcano | notices have no coordinates |
| usgs_hans | alert_level | event | severity | NORMAL 1, ADVISORY 2, WATCH 3, WARNING 4 | severity_unit = 'USGS volcano alert level' |
| usgs_hans | notice_url | event | raw_reference | text | + content_hash |
| usgs_hans | vnum | volcano_alert | volcano_number | int; null -> quarantine `region_level_notice`; '0'/'999999' -> `invalid_vnum` |  |
| usgs_hans | alert_level / color_code | volcano_alert | alert_level / color_code | upper() | ground hazard / aviation hazard |
| usgs_hans | notice_type_cd, obs_abbr | volcano_alert | notice_type, observatory | text |  |
| usgs_hans | (previous notice of the vnum) | volcano_alert | previous_alert_level / previous_color_code | lag over sent_utc | detects raised / lowered levels |
| usgs_hans (getUSVolcanoes) | vnum, name, latitude, longitude, nvews_threat, obs_abbr | volcano | (same) | only for vnums missing from the GVP Holocene list (`is_holocene = false`) | e.g. Yellowstone 325010, Long Valley 323822 |

## Domain tables

- **volcano** — static geographic entity: 165 GVP Holocene volcanoes with `Country = 'United States'`, plus the HANS volcanoes that GVP keeps outside the Holocene list (Yellowstone, Long Valley, ...). Not an event; it is the anchor for every spatial join.
- **volcano_alert** — official USGS notices from HANS: alert level, aviation colour code, previous level, observatory, notice URL. The view `volcano_current_alert` gives the current state of every volcano (NORMAL/GREEN when no notice exists).
- **eruption** — GVP eruption attributes (VEI, start/end with uncertainty, activity type, evidence).
- **earthquake** — magnitude type, depth, review status, network, USGS `updated`.
- **volcano_earthquake** — many-to-many proximity link (an earthquake can be near two volcanoes; this is expected, not a duplicate).
- **activity_report** — weekly report entries of US volcanoes (optional source: week range, NEW/ONGOING, text).

See the ER diagram in [`../docs/er_diagram.png`](../docs/er_diagram.png) and the DDL draft in [`../sql/001_schema_draft.sql`](../sql/001_schema_draft.sql).

> Field names were verified against real responses on 2026-11-09 (see the profiling notebook).
