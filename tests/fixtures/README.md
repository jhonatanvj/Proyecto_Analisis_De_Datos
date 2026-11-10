# Recorded fixtures

Real responses used by pytest instead of live calls (no test touches the network).

| File | Recorded (UTC) | Source |
|---|---|---|
| `usgs_hans_monitored.json` | 2026-11-10 02:49 | USGS HANS `volcano/getMonitoredVolcanoes` |
| `gvp_weekly_rss.xml` | 2026-11-10 02:49 | Smithsonian / USGS Weekly Volcanic Activity Report RSS (ISO-8859-1) |

To refresh: run `python scripts/fetch_samples.py` and copy the new files from `workshop_1/samples/`.
