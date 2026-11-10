# Source status log

Short log of availability/format changes observed in the sources (required by the course).
Report any change to the professor as soon as it is detected.

| Date (UTC) | Source | Observation | Action |
|---|---|---|---|
| 2026-11-09 | GVP VOTW | Current database version documented as v5.4.0 (7 Aug 2026), DOI 10.5479/si.GVP.VOTW5-2026.5.4 | Cite this version; verify with the version shown at download time |
| 2026-11-09 | Project | Scope changed from global to United States | sources.yaml, mapping and contract updated |
| 2026-11-10 | GVP WFS | Eruptions layer has no Latitude/Longitude properties (only GeoJSON geometry); `CQL_FILTER` works on both layers | Coordinates taken from geometry; US subset via `Country` / `Volcano_Number IN (...)` |
| 2026-11-10 | Weekly report RSS | Encoding ISO-8859-1; `<link>` is now generic (`reports_weekly.cfm`), vnum only in `<guid>` (`#vn_311120`); status suffix is `- New/Continuing Eruptive Activity`; feed not updated since 15 Oct 2026 (Last-Modified) | Parser updated; source demoted to optional; freshness tracked |
| 2026-11-10 | USGS HANS | Endpoints verified: `getMonitoredVolcanoes` (69), `getElevatedVolcanoes` (3), `getUSVolcanoes` (170), `getVolcano/{vnum}`, `notice/getNotice/{id}`. Placeholder vnums `0`/`999999`; region rows with `vnum = null`; 13 volcanoes without notice (`sent_unixtime = 0`) | Promoted to main incremental source; validation rules in `usgs_hans.reject_reason` |
| 2026-11-10 | USGS FDSN | M ≥ 1.0, 30 km, 2010 → today: 365,100 rows for 165 volcanoes; Kilauea (87,679) and Clear Lake (63,265) exceed the 20,000 per-request limit | Partition those by year (month if needed) |
