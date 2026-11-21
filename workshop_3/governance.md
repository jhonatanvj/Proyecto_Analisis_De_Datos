# Workshop 3, Part 2: data quality findings and governance KPIs

Source text for the report sections *Data Quality Findings* and *Governance KPI Definition*. The numbers come from `notebooks/03_data_quality_findings.ipynb` (pandas, Silver measured on 2026-11-19 23:28 UTC) and from the Gold file `datalake_gold/governance_20261119_234142.parquet` (PySpark, `gold_processing` run of 2026-11-19 23:41 UTC). Finding ids (F01-F24) are the index of the `findings` table in section 9 of the notebook.

## 1. Data Quality Findings

### 1.1 What was measured

The Silver layer after the historical backfills and two days of hourly runs: 7 topics in `datalake_silver/<topic>/`, 17 Parquet files.

| Topic | Rows | Distinct records | Time span |
|---|---:|---:|---|
| `earthquakes` | 292,264 | 291,595 | 2010-01-01 to 2026-11-19 |
| `notices` (HANS notice texts) | 16,537 | 16,537 | 2010-01-01 to 2026-11-19 |
| `eruptions` | 1,281 | 1,281 | year 10 (BCE dates kept in `start_year`) to 2025-10-20 |
| `volcanoes` | 165 | 165 | static catalogue |
| `alert_notices` | 56 | 56 | latest HANS notice per volcano |
| `volcano_updates` (scraped page) | 12 | 12 | 2026-11-02 to 2026-11-19 |
| `quarantine` | 2,108 | - | rejected records with their reason |

Rates are computed on the newest version of each record (`latest()`), except the duplicate measures, which need every version.

### 1.2 Findings

| Id | Field | Issue | Frequency / severity | Handling | Monitored by |
|---|---|---|---|---|---|
| F01 | `eruptions.vei` | Missing values: GVP has no VEI for the eruption | 37.5 % | Flag `vei_missing`, not imputed | `null_rate` |
| F02 | `notices.alert_level` | Missing values: VONA notices carry only the aviation colour code | 7.9 % | Kept; colour code available | `null_rate` |
| F03 | `notices.region` | Missing values: region-wide notices have no single volcano | 11.4 % | Flag `is_region_wide` | `null_rate` |
| F04 | `alert_notices.latitude/longitude` | Missing coordinates in HANS notices | 7.1 % | Flag `coords_missing` | `null_rate` |
| F05 | Lineage columns | Gaps in `bronze_file`, `bronze_sha256`, `processed_utc` | None | Required by the Silver writer | `null_rate` |
| F06 | `earthquakes` | Real revisions: USGS changed a value of a stored event | 20 rows (0.007 %) | Kept; Gold uses the newest version | `duplicate_rate` |
| F07 | `earthquakes` | Same event re-written with another `content_hash` although no value changed: the hash depends on the source format (`usgs_feed` vs `usgs_fdsn`) | 371 rows | Gold uses `latest()`, counts not affected; fix proposed for Silver | `duplicate_rate` |
| F08 | `earthquakes` | Exact copies: the dedup index keeps only the last hash per id, so the sequence feed → FDSN → feed admits the first hash again | 278 rows (0.095 %), growing every hour | Pipeline bug in `silver/state.py`, reported to Part 1 | `duplicate_rate` |
| F09 | `usgs_fdsn` (Bronze) | Same event returned by the radial queries of neighbouring volcanoes (30 km circles overlap); one event appears 9 times | 72,914 of 368,464 records (19.8 %) | Removed by the Silver dedup index | `duplicate_rate` |
| F10 | `notices.notice_identifier` | One identifier shared by an activity notice (VAN) and its aviation version (VV) | 706 identifiers | Key = identifier + notice type; both kept | `duplicate_rate` |
| F11 | Bronze files | Files that added no row to Silver (all records already seen or out of scope) | 8 of 387 files | Dedup index and scope filter | `duplicate_rate` |
| F12 | Silver files | Files whose schema differs from the contract | 0 of 17 | `schemas.enforce()` before writing | `schema_compliance` |
| F13 | `usgs_feed`, `usgs_fdsn` | Non-tectonic events (quarry blasts, explosions) | 2,089 records | Quarantine `non_tectonic_type` (business rule) | `quarantine_rate` |
| F14 | `usgs_hans` | Volcano without notice, region-level notice | 15 records | Quarantine (business rule) | `quarantine_rate` |
| F15 | `usgs_hans_notices` | Notice without identifier or sent time | 4 records | Quarantine `missing_id_or_time` (source defect) | `quarantine_rate` |
| F16 | Quarantine | HANS reference file quarantined as `unknown_source` | 0 records in this run | Pipeline bug fixed on 2026-11-19 | `quarantine_rate` |
| F17 | `earthquakes.depth_km` | IQR outliers: deep Alaska slab events, ~30 km Hawaii events | 9.4 % with per-volcano fences (Silver flags 5.7 %, global fences 16.7 %; 27 % in the small Workshop 2 sample) | Flagged, not removed: real events | `outlier_rate` |
| F18 | `earthquakes.mag` | IQR outliers | 3.2 % with per-volcano fences (Silver flags 3.8 %) | Flagged, not removed | `outlier_rate` |
| F19 | `earthquakes.is_outlier_*` | Silver computes the fences inside each Bronze file: the outlier threshold goes from M1.75 to M5.20 across 101 batches, so M1.75 is an outlier in one file while values up to M5.10 are normal in another | Inconsistent across batches | Gold recomputes the rate with per-volcano fences; Silver flags unchanged | `outlier_rate` |
| F20 | `notices.text_length` | Long, uneven bodies | Median 2,077 characters, mean 2,781, max 16,965 | Footer and boilerplate removal, summary extraction (Part 1) | `text_length` |
| F21 | `notices.text_clean` | Encoding errors (mojibake, UTF-8 decoded twice): `kä lauea` for Kīlauea, `volcanoâ tms` for volcano’s, `â1 4 mile` for ¼ mile | 9 notices (0.054 %) | Not fixed yet; re-decoding proposed for the Silver text step | `text_length` |
| F22 | `notices.text_clean` | Single language (English); the only non-ASCII characters are Hawaiian vowels and a few accented words (Társilo, décollement) | 0.106 % of the characters | Language-distribution KPI excluded | `text_length` |
| F23 | Bronze files of hourly sources | Hours without a new file: Bronze writes a file only when the source content changed | 1 to 2 hours with a file per day in the first two days | Measured as hours with a new file per day, not files per run | `ingestion_frequency` |
| F24 | All sources | Age of the newest Bronze file | 0.7 h for hourly sources, 70 h for the monthly GVP catalogue | Expected age depends on the schedule in `sources.yaml` | `freshness` |

### 1.3 Supporting statistics and figures

All figures are produced by the notebook and saved in `workshop_3/figures/`.

| Figure | Shows | Findings |
|---|---|---|
| `dq_null_rate.png` | The 15 fields with the highest missing rate | F01-F04 |
| `dq_quarantine.png` | Quarantined records per source and reason | F13-F16 |
| `dq_outliers.png` | Depth and magnitude histograms (log scale) with the IQR outliers highlighted | F17-F19 |
| `dq_text_length.png` | Distribution of the notice length and median length per year | F20 |
| `dq_volume_weekly.png` | Earthquakes and notices per week | context for every rate |
| `dq_ingestion_frequency.png` | Hours with a new Bronze file per day for the hourly sources | F23 |

Descriptive statistics of the earthquakes (newest version, 291,595 events): magnitude median 1.71 (1st-99th percentile 1.0-3.8, max 7.9); depth median 3.4 km (99th percentile 154 km, max 584 km); distance to the nearest volcano median 16 km (max 30 km, the scope limit). Notice length per observatory (median characters): HVO 3,923, YVO 2,471, CalVO 1,852, AVO 1,588, NMI 1,165, CVO 904.

### 1.4 Experimentation: what changed while measuring

* **The "legitimate revisions" were mostly a bug.** The first version of the notebook reported 389 extra versions of earthquakes and called them USGS revisions. Comparing the values of each version showed that only 20 change anything; the rest are the same event written again because the feed and FDSN produce different hashes (F07), and an hourly Silver run then added 278 exact copies (F08). The duplicate KPI was split into exact copies, same-value copies and real revisions because of this.
* **A small sample misleads.** The Workshop 2 sample (709 earthquakes) gave 27 % depth outliers; the full history gives 5.7 % with the Silver flags.
* **Global fences are not the answer either.** Recomputing the IQR fences over all earthquakes flags 16.7 % of the depths, because every deep Alaska slab event falls outside them. Fences per volcano (with global fences for the 33 volcanoes with fewer than 30 earthquakes) give 9.4 % and compare each volcano with its own seismicity, as user story US3 does.
* **Two false alarms in the schema check.** The first comparison flagged every file: Silver writes timestamps in microseconds on purpose (Spark 3.5 cannot read nanoseconds), and Parquet names the list child `element` where Arrow says `item`. Both are normalised before comparing.
* **Pending files distort the funnel.** Bronze files written after the last Silver run looked like "files that added no row"; only files listed in the Silver manifest are counted now.
* **Not every accent is an error.** Of 15 notices with unexpected characters, 6 are legitimate (a person's name, a French geology term); only the patterns of double-decoded UTF-8 count as encoding errors.

## 2. Governance KPI Definition

Six data-quality KPIs measure the data; three operational metrics measure how the pipeline delivers it. Each indicator is one function in `src/volcano_pipeline/gold/governance.py`, tested in `tests/test_gold_governance.py`, and becomes one or more `section` values of the Gold governance file. The file uses the shared long format `section, metric, dimension, period, value, unit, generated_utc` (`gold/contract.py`), so each indicator maps to one chart or KPI card of the Workshop 4 governance dashboard.

| Indicator | Type | Quality dimension (DAMA-DMBOK) | Findings |
|---|---|---|---|
| `null_rate` | quality KPI | Completeness | F01-F05 |
| `duplicate_rate` | quality KPI | Uniqueness | F06-F11 |
| `schema_compliance` | quality KPI | Validity (structure) | F12 |
| `quarantine_rate` | quality KPI | Validity (business rules) | F13-F16 |
| `outlier_rate` | quality KPI | Accuracy / plausibility | F17-F19 |
| `text_length` | quality KPI | Consistency | F20-F22 |
| `volume` | operational metric | - | context |
| `freshness` | operational metric | Timeliness | F24 |
| `ingestion_frequency` | operational metric | Timeliness | F23 |
| `language_distribution` | excluded | - | F22 |

### 2.1 `null_rate` (completeness)

* **Definition:** percentage of missing values per field of each Silver topic. Missing = null, blank string or empty list.
* **Formula:** `100 × missing(column) / records`, on the newest version of each record.
* **PySpark:** one `agg` with an `avg(is_missing.cast("double"))` expression per column (one pass over the data), then `unpivot` to one row per field.
* **Gold rows:** `section = null_rate`, `metric = pct_null`, `dimension = <topic>.<column>`, `unit = %` (124 rows).
* **Result:** `eruptions.vei` 37.5 %, `notices.region` 11.4 %, `notices.alert_level` 7.9 %, `alert_notices.latitude` 7.1 %; lineage columns 0 %.
* **Justification:** F01-F04 are known, explained gaps that must stay stable; a jump would mean a source changed its format. F05 is a control: lineage must always be 0 %.
* **Dashboard:** horizontal bar chart of the 15 fields with the highest rate.

### 2.2 `duplicate_rate` (uniqueness)

* **Definition:** duplicates before deduplication (what the sources send) and after it (what Silver keeps), with the extra rows of a record split in three classes.
* **Formulas, after dedup, as % of the topic rows:**
  * exact copies = rows − distinct (`record_id`, `content_hash`);
  * same-value copies = rows − distinct (`record_id`, hash of the value columns); includes the exact copies;
  * real revisions = distinct (`record_id`, values) − distinct `record_id`.
  * The value columns exclude the key, the hash, the lineage columns and the per-batch outlier flags.
* **Formula, before dedup:** `100 × (FDSN records − distinct FDSN ids) / FDSN records` over the Bronze files already processed by Silver.
* **PySpark:** three `countDistinct` per topic, with `xxhash64` over the value columns. For the Bronze part, `spark.read.json` with an explicit schema that keeps only `payload.features.id` (no inference over 368,000 GeoJSON features), then `explode`.
* **Gold rows:** `section = duplicate_rate`; `metric = pct_exact_copies | pct_same_value_copies | pct_real_revisions` per topic; `pct_repeated_before_dedup`, `records_before_dedup` and `repeated_before_dedup` for `usgs_fdsn`.
* **Result:** earthquakes 0.095 % exact copies, 0.222 % same-value copies, 0.007 % real revisions; FDSN 19.8 % repeated before dedup; every other topic 0 %.
* **Justification:** F06-F11. The KPI was planned as a preventive control (0 expected) and found a real bug on its first run (F08).
* **Exception to "Gold reads Silver":** this is the only indicator that reads Bronze. Duplicates removed by the dedup index cannot be seen in Silver.
* **Dashboard:** grouped bars, before vs after dedup.

### 2.3 `schema_compliance` (validity, structure)

* **Definition:** percentage of Silver files whose columns (names, order) and types are exactly the contract in `silver/schemas.py`.
* **Formula:** `100 × conforming files / files`, per topic.
* **PySpark:** `df.inputFiles()` lists the files of each topic folder; `spark.read.parquet(file).schema` reads only the file footer. The contract is translated from pyarrow to Spark types; timestamp units and list child names are normalised (section 1.4).
* **Gold rows:** `section = schema_compliance`, `metric = pct_files_conforming | files`, `dimension = <topic>`.
* **Result:** 100 % in every topic (17 files).
* **Justification:** F12. `spark.read.parquet(<folder>)`, the call the whole Gold layer relies on, needs one schema per folder; the Workshop 3 brief asks for consistent schemas across runs. 100 % today, so it is a preventive control.
* **Dashboard:** one KPI card per topic (green at 100 %).

### 2.4 `quarantine_rate` (validity, business rules)

* **Definition:** percentage of the processed records that were sent to quarantine, per source. The reasons are a breakdown, grouped in three categories:
  * `business_rule`: out of scope by design;
  * `source_defect`: unusable record from the source;
  * `pipeline_error`: our bug.
* **Formula:** `100 × quarantined / (Silver rows + quarantined)` per source and for all sources, counting every version on both sides.
* **PySpark:** source extracted from the `bronze_file` lineage column with `regexp_extract`; full outer `join` of Silver counts and quarantine counts; category from a `create_map` lookup.
* **Gold rows:** `section = quarantine_rate`; `metric = pct_quarantined` per source plus `all sources`; `metric = records_by_reason` with `dimension = source|reason|category`.
* **Result:** 0.67 % for all sources. `usgs_hans` 21.1 % and `usgs_feed` 18.2 %, all business rules; `usgs_fdsn` 0.66 %; `usgs_hans_notices` 0.02 % (source defects); 0 pipeline errors.
* **Justification:** F13-F16. A high rate is not bad by itself: the categories separate expected rejections from errors.
* **Dashboard:** stacked bars per source coloured by category.

### 2.5 `outlier_rate` (accuracy / plausibility)

* **Definition:** percentage of earthquakes whose magnitude or depth is outside the IQR fences of their volcano.
* **Formula:** fences `[Q1 − 1.5 × IQR, Q3 + 1.5 × IQR]`, computed over all the records of each volcano (`nearest_vnum`); global fences for volcanoes with fewer than 30 earthquakes.
* **PySpark:** `percentile_approx(col, [0.25, 0.75])` per volcano (`groupBy`) and overall; `join` and `crossJoin`; `when` picks the volcano or the global fences.
* **Gold rows:** `section = outlier_rate`, `metric = pct_outlier | pct_outlier_silver_flags`, `dimension = earthquakes.mag | earthquakes.depth_km`.
* **Result:** magnitude 3.2 % (Silver flags 3.8 %), depth 9.4 % (Silver flags 5.7 %).
* **Justification:** F17-F19. Silver's per-file flags are not comparable between runs, so Gold recomputes the rate with one consistent rule without changing Silver. Outliers are monitored, never deleted: they are real deep or large events.
* **Dashboard:** bars per field, Gold rate next to the Silver flags.

### 2.6 `text_length` (consistency)

* **Definition:** length of the cleaned notice text in characters, plus the sub-metric `pct_encoding_errors`.
* **Formula:** mean, median, minimum and maximum of `text_length` per observatory and year, per observatory and overall. Encoding errors = `100 × notices matching [âäåœïÃ€™] / notices`.
* **PySpark:** `groupBy(observatory, trunc(sent_time, "year"))` with `avg`, `percentile_approx(…, 0.5)`, `min` and `max`, then `unpivot`; `rlike` for the encoding check.
* **Gold rows:** `section = text_length`, `metric = mean_chars | median_chars | min_chars | max_chars | pct_encoding_errors`, `dimension = <observatory> | notices | volcano_updates`, `period` = year.
* **Result:** median 2,076 characters; encoding errors 0.054 %.
* **Justification:** F20-F22. The cleaning of Part 1 removed 27 % of repeated text; a sudden change of length per observatory would show that a template changed or the cleaning broke. Encoding errors belong here because they are a text-content defect, not a separate dimension.
* **Dashboard:** line per observatory over the years, plus a card for the encoding rate.

### 2.7 Operational metrics

| Metric | Definition | Gold rows | Result | Justification / dashboard |
|---|---|---|---|---|
| `volume` | Records per topic, all-time and per week (newest version) | `volume_total` (`dimension` = topic) and `volume_weekly` (`period` = week) | 291,595 earthquakes, 16,537 notices | Context and denominator of every rate. KPI cards and a weekly line |
| `freshness` | Hours since the newest Bronze file that reached Silver, per source | `section = freshness`, `metric = hours_since_last` | 0.7 h for hourly sources, 70 h for the monthly GVP catalogue | F24, user story US4 (*how fresh is the data?*). Cards with a traffic light against the schedule |
| `ingestion_frequency` | Hours with a new Bronze file per source and day vs the runs expected from `sources.yaml` | `section = ingestion_frequency`, `metric = hours_with_file | expected_per_day | pct_of_expected`, `period` = day | 1-2 hours with a file per day in the first two days | F23. Bronze writes only when the source changed, so the measure is hours with a file, not files. Bars of expected vs actual per day |

`freshness` and `ingestion_frequency` read the `bronze_file` lineage column, so Bronze files that added no row (F11) are not seen. This is acceptable because those files carried no new data.

### 2.8 Excluded: `language_distribution`

The Workshop 3 brief suggests it for multilingual data. All sources are English publications of the USGS and the Smithsonian; non-ASCII characters are 0.106 % of the text, and they are Hawaiian vowels, accented names and encoding errors (F22). A language breakdown would always show 100 % English, so it is not computed. The encoding problems that a language detector might have revealed are covered by `text_length.pct_encoding_errors`.

### 2.9 Issues reported to the pipeline (Part 1)

| Issue | Finding | Proposed fix |
|---|---|---|
| Feed and FDSN hashes differ for the same event, and the dedup index keeps only the last hash | F07, F08 | Hash the normalised value columns, or accept a new version only when `updated_time` increases |
| Outlier fences computed per Bronze file | F19 | Compute the flags per volcano over the topic, as Gold does |
| Mojibake in 9 notices | F21 | Repair double-decoded UTF-8 in the Silver text step (re-encode as cp1252 / Latin-1 and decode as UTF-8 when the pattern matches) |
| `generated_utc` shifted by the host UTC offset outside Docker | - | Fixed in `gold/contract.py` (pass a time-zone-aware datetime to Spark), with a test |

## 3. References

* Apache Spark 3.5 documentation, PySpark SQL API: `DataFrame.unpivot`, `functions.percentile_approx`, `functions.xxhash64`, `DataFrameReader.json`. https://spark.apache.org/docs/3.5.4/api/python/
* DAMA International (2017). *DAMA-DMBOK: Data Management Body of Knowledge*, 2nd ed., chapter 13 (Data Quality): quality dimensions.
* ISO/IEC 25012:2008, *Software engineering — Software product Quality Requirements and Evaluation (SQuaRE) — Data quality model*.
* Tukey, J. W. (1977). *Exploratory Data Analysis*. Addison-Wesley (IQR fences).
* Global Volcanism Program (2026). *Volcanoes of the World*, v5.4.0. Smithsonian Institution. doi:10.5479/si.GVP.VOTW5-2026.5.4
* U.S. Geological Survey: Earthquake Hazards Program (ComCat FDSN event service and real-time GeoJSON feeds) and Volcano Hazards Program (HANS public API). Public domain.
