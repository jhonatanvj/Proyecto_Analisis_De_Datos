# Silver preprocessing pipeline (first version)

Every rule below responds to something observed in the Workshop 1 samples or in the first real runs of the DAGs on 2026-11-16. Code: `src/volcano_pipeline/silver/`. Counts come from the first full Silver run (5 Bronze files, 2026-11-16 22:41 UTC) and the exploration notebook `notebooks/02_silver_exploration.ipynb`.

## Topics and target schema

| Topic | From Bronze source | Rows (first run) | Key (`record_id`) |
|---|---|---|---|
| `earthquakes` | `usgs_feed` | 229 (of 1,811 features in the feed) | `usgs:<id>` (same id space as FDSN) |
| `alert_notices` | `usgs_hans` | 54 (of 69 rows) | `usgs_hans:<vnum>:<notice_identifier>` |
| `volcanoes` | `gvp_volcanoes` | 165 | `gvp_volcano:<vnum>` |
| `eruptions` | `gvp_eruptions` | 1,281 (US subset of 11,089) | `gvp_eruption:<Eruption_Number>` |
| `volcano_updates` | `usgs_volcano_updates` (scraped) | 9 | `usgs_vu:<type>:<observatory>:<volcano>:<notice url / number / issue time>` |

The exact schema of each topic (names, Arrow types, categorical columns) is in `silver/schemas.py`. It is enforced on every write: missing columns are added as nulls, unknown columns are dropped and types are cast, so all files of a topic share one schema. Every row also carries `content_hash` (SHA-256 of the source record) and three lineage columns: `bronze_file`, `bronze_sha256` and `processed_utc`.

## Data types

| Field type | Rule | Why |
|---|---|---|
| Event times | `timestamp[ns, UTC]`. Feed `time` and `updated` are epoch milliseconds, HANS `sent_unixtime` is epoch seconds | Workshop 1 finding: three different time encodings across sources |
| Eruption start | `timestamp[us, UTC]` | Nanosecond timestamps stop at 1677. The first test run failed on eruption 20745 (year 1510) |
| Notice times on the scraped page | Parsed with an explicit table of US time zones (AKST, HST, PST, MST, ChST...) | The page mixes local times ("10:48 AM AKST") with UTC; without the table, a parser would read AKST times as UTC (9 h error) |
| Alert level, colour code, magType, network, review status, observatory, time precision, activity type | `category` (Arrow dictionary) | Few distinct values, repeated in every row |
| Volcano numbers, years, VEI | Nullable `Int64` | GVP leaves VEI and years empty; plain integers cannot hold nulls |

## Null handling

| Field | Observed | Strategy |
|---|---|---|
| Eruption VEI | Missing in 480 of 1,281 US eruptions (37%) | Keep NULL, `vei_missing = True`. **Not imputed**: a guessed explosivity would distort the eruptive component of the indicator |
| Eruption month/day | 0 or null in many eruptions (232 with year precision, 5 with month precision) | First day of the known period + `time_precision` column (`day`, `month`, `year`) |
| Eruption year < 1 (BCE) | 387 eruptions | `start_time = NULL`, `time_precision = 'bce'`, the year is kept in `start_year` |
| `Last_Eruption_Year` | Null in the WFS response (older exports used the text 'Unknown') | Both map to NULL |
| Feed `mag` | No nulls this week; USGS documents it as nullable | Keep the row, `mag_missing = True`, NULL severity. Covered by a recorded test case |
| Feed geometry | Required for the volcano join | Missing → quarantine `missing_coordinates` |
| HANS `vnum` null (region-wide notices such as "Alaskan Volcanoes") | 2 rows | Quarantine `region_level_notice` |
| HANS volcano with no notice (`notice_identifier` null, `sent_unixtime` 0) | 13 rows | Quarantine `no_notice_issued`. The volcano keeps its default NORMAL/GREEN state; it is not an event |
| HANS volcano outside the 165 GVP Holocene volcanoes (e.g. Yellowstone) | coordinates unknown | Keep, `coords_missing = True` |
| Optional notice fields (`notice_number`, `notice_url`, `vnum` on the scraped page) | depend on the observatory format | Keep NULL; the record id falls back to the issue time |

## Validation and scope

| Rule | Action |
|---|---|
| Latitude in [-90, 90]; longitude normalised to [-180, 180) | Out of range → quarantine |
| Depth in [-5, 800] km | Negative depths are valid at volcanoes (hypocentre above sea level; seen in the Workshop 1 FDSN sample). Outside the range → quarantine |
| Magnitude in [-2, 10] | Outside → quarantine |
| Feed `type` must be `earthquake` | 129 records this week (quarry blasts, explosions, ice quakes, other events) → quarantine `non_tectonic_type` |
| Earthquake within 30 km of a US volcano and M ≥ 1.0 | Others are counted (`out_of_scope`, 1,453 in the first run) but not stored. Radius and magnitude come from `config/sources.yaml` |
| Eruption of a volcano in the GVP US list | The eruptions layer has no country field, so scope is applied through the volcano number |
| Scraped record with no text after cleaning | Quarantine `empty_text` |
| Bronze file that is not valid JSON, or an unknown source | Whole file → quarantine (`unreadable_bronze`, `unknown_source`); the run continues |

## Outliers

Outliers are **flagged, never removed**. Earthquake swarms and very shallow events near a volcano are the signal the project looks for, so deleting them would hide exactly what the user needs to see.

| Column | Method | Flagged in the first run |
|---|---|---|
| `mag` | IQR (1.5 × IQR beyond Q1/Q3) → `is_outlier_mag_iqr` | 3 |
| `mag` | Modified z-score (0.6745·(x − median)/MAD > 3.5, Iglewicz and Hoaglin) → `is_outlier_mag_z` | 2 |
| `depth_km` | IQR → `is_outlier_depth_iqr` | 31 |
| `depth_km` | Modified z-score → `is_outlier_depth_z` | 62 |

The modified z-score uses the median and the MAD instead of the mean and the standard deviation because the distributions are skewed (many small shallow events, a few deep ones), and a plain z-score would be pulled by the very values it should detect. The 62 depth outliers (z-score) are split between the Alaska network (32, many of them subduction-slab events deeper than 100 km that fall within 30 km of a volcano in map view) and the Hawaii network (29, events around 30 km deep under Kīlauea and Mauna Loa). Workshop 3 can use the flag to separate shallow volcanic seismicity from deep tectonic events.

## Deduplication

| Level | Criterion |
|---|---|
| Within one Bronze file | Last version per `record_id` (sorted by `updated`) |
| Across Bronze files of one run | Same, files processed oldest first |
| Across runs | `_state/seen_<topic>.parquet` keeps the latest `content_hash` per `record_id`. A record is written again only if its hash changed (for example a USGS event reviewed and relocated). The USGS feed is a rolling 7-day window, so without this each event would be stored up to 168 times |
| Quarantine across runs | Same mechanism with key `source:reason:hash(payload)`. Found in the first real reruns: the same 129 non-tectonic events came back every hour |
| Bronze files | The Silver manifest `_state/manifest.json` lists processed files; a file is never processed twice |

Re-running either DAG for the same period therefore produces no duplicate rows (tested in `tests/test_silver_run.py`).

## Text preprocessing (scraped USGS notices)

Applied to the synopsis and activity text of each scraped block (`silver/text.py`):

1. Strip HTML with BeautifulSoup. The real text contains `<p>`, `<strong>`, `<sub>` (SO<sub>2</sub>) and `&nbsp;`.
2. Unicode NFKC normalisation. This keeps Hawaiian names with macrons (Kīlauea, Uēkahuna, Pāhala) and turns non-breaking spaces into normal spaces.
3. Lowercase.
4. Remove URLs. Notices end with links to the NWS, ash advisories and observatory pages.
5. Replace punctuation with spaces and collapse whitespace.
6. Tokenise on Unicode letters and digits; drop one-character tokens, digit-only tokens and English stop words. The list is in `config/stopwords_en.txt`, a fixed file with no runtime downloads.
7. Keyword flags by word stem: `kw_eruption` (erupt*), `kw_ash`, `kw_lava`, `kw_seismicity` (seismic*), `kw_deformation` (deform*), `kw_gas`, `kw_plume`.

Output columns: `text_clean`, `tokens`, `token_count`, `kw_*`. Most frequent tokens in the first run: volcanic, activity, episode, eruption, fountaining, november, summit, lava. Words such as *november*, *data* and *recorded* carry no meaning for the user, so a domain stop-word list is planned for Workshop 3.

## Known limitations of this first version

* The feed covers 7 days; history comes from FDSN (Workshop 3).
* Elevated blocks on the scraped page have a volcano name but no VNUM; matching names to volcano numbers is left for Workshop 3.
* Outlier flags are computed per batch, so a single-event batch never flags anything. Workshop 3 will compute them over the accumulated Silver data with PySpark.
