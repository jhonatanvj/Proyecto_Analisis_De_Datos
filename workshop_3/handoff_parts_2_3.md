# Workshop 3 hand-off: how to build Parts 2 and 3

For Julián (Part 2, governance) and Juan Sebastián (Part 3, storytelling). Part 1 left a working Gold DAG that writes both Parquet files with one example metric each; your job is to add the KPIs and aggregations. Plan and split: [`plan.md`](plan.md). What Part 1 changed and why: [`pipeline_improvements.md`](pipeline_improvements.md).

## 1. Set up your clone (once)

```bash
git fetch && git checkout workshop-3          # or main, once workshop-3 is merged
git checkout -b w3-governance                 # Part 2   (Part 3: w3-storytelling)
docker compose up -d --build                  # new image: Java 17 + PySpark; db-migrate creates the tables
```

Then in Airflow (http://localhost:8080) unpause `bronze_ingestion` and `silver_processing`, and trigger once by hand:

1. `bronze_backfill_hans_notices` (HANS notice archive since 2010, ~20 min),
2. `bronze_backfill_fdsn` (earthquakes since 2010, longer; optional for Part 3 if you only need the notices).

`silver_processing` turns the new Bronze files into Silver within the hour (or trigger it). Check with `python ingest.py --mode status`.

## 2. What Silver gives you (real counts, 2026-11-19)

Each topic is one folder, read with `spark.read.parquet("datalake_silver/<topic>/")` through `read_silver()`.

| Topic | Rows | Useful columns |
|---|---|---|
| `earthquakes` | 292,239 rows, 291,571 earthquakes | `event_time`, `updated_time`, `mag`, `mag_type`, `depth_km`, `network`, `review_status`, `nearest_vnum`, `distance_km`, `mag_missing`, `is_outlier_mag_iqr/_z`, `is_outlier_depth_iqr/_z` |
| `eruptions` | 1,281 | `vnum`, `start_time` (null before year 1), `time_precision` (day/month/year/bce), `start_year`, `vei`, `vei_missing`, `activity_type` |
| `volcanoes` | 165 | `vnum`, `name`, `primary_type`, `last_eruption_year`, `elevation_m`, `latitude`, `longitude` |
| `alert_notices` | 55 | latest HANS notice per volcano: `vnum`, `alert_level`, `alert_level_ord` (1-4), `color_code`, `sent_time`, `coords_missing` |
| `notices` | 16,535 | HANS notice texts 2010-2026: `sent_time`, `observatory`, `notice_type` (DU/WU/SR/VAN/VV/IS/MU/BU), `vnum`, `vnums`, `volcano_name`, `region`, `is_region_wide`, `alert_level`, `alert_level_ord`, `color_code`, **`summary_clean`, `summary_tokens`**, `text_clean`, `tokens`, `token_count`, `text_length`, `summary_length`, `kw_*` |
| `volcano_updates` | 12 | scraped Volcano Updates page: `record_type`, `alert_level`, `text_clean`, `tokens`, `kw_*` |
| `quarantine` | 2,107 | `source_id`, `reason`, `payload` (JSON string) |

Every topic also has `record_id`, `content_hash` and the lineage columns `bronze_file`, `bronze_sha256`, `processed_utc`.

**Silver keeps versions.** A record that changes at the source (USGS revises a magnitude, the feed repeats a week) is written again in a later file with the same `record_id`. Before counting records, keep the newest version with `latest()` from `gold/silver_reader.py` (the example `earthquakes_weekly` does). The gap between rows and distinct ids is itself a governance KPI: 668 earthquake versions (0.23 %).

## 3. Add a KPI or an aggregation

One small function per number family, in `src/volcano_pipeline/gold/governance.py` (Part 2) or `gold/storytelling.py` (Part 3), added to the `KPIS` / `AGGREGATIONS` list at the bottom. It receives `spark` and the dict of Silver DataFrames (a topic is `None` when it has no file yet) and returns a DataFrame with at least `section`, `metric`, `value`, plus `dimension`, `period` and `unit` when useful. `conform()` adds the missing columns, casts the types and stamps `generated_utc`.

```python
def vei_missing_rate(spark, silver: dict):
    """37 % of GVP eruptions have no VEI (Workshop 2 finding) -> KPI 'null_rate'."""
    from pyspark.sql import functions as F

    er = silver.get("eruptions")
    if er is None:
        return None
    return er.agg((F.avg(F.col("vei_missing").cast("double")) * 100).alias("value")).select(
        F.lit("null_rate").alias("section"), F.lit("pct_null").alias("metric"),
        F.lit("eruptions.vei").alias("dimension"), "value", F.lit("%").alias("unit"))

KPIS = [volume_total, vei_missing_rate]
```

Conventions:

* `section` = the chart or card the rows feed in Workshop 4 (`null_rate`, `volume_weekly`, `tone_weekly`, `top_keywords`...). Agree the names with each other before Workshop 4.
* `period` = start of the week (`F.date_trunc("week", ...)`) or day; leave it out for all-time values.
* Keep the output small (one row per bar or point); the dashboards read it with pandas.
* `build_storytelling` already skips a function that returns `None`; do the same in `build_governance` if a KPI can return `None`.

## 4. Test and run

* Unit tests go in `tests/test_gold.py` (or a new `tests/test_gold_<part>.py`) using the `spark` and `silver_dir` fixtures there (Workshop 2 Silver samples in the new layout). Notice fixtures (six real notices) are in `tests/fixtures/hans_notices/`.
* Spark needs Java 8/11/17. On the host the Spark tests skip themselves if Java does not fit; run them in the image:

```bash
docker compose run --rm airflow-scheduler pytest -q tests/test_gold.py
docker compose run --rm airflow-scheduler airflow dags test gold_processing     # full run, ~3-5 min
```

* Look at the result: `pd.read_parquet("datalake_gold/governance_<stamp>.parquet")`.
* `pytest -q` must pass before the pull request (rules in `plan.md`).

## 5. Part-specific notes

**Part 2 (governance).** Real findings to justify KPIs, all reproducible from Silver or the database:

* `eruptions.vei` null 37.5 %; `notices.alert_level` null 7.9 % (VONA notices carry only a colour code); `notices.region` null 11.4 % (region-wide notices).
* Quarantine 2,107 records: mostly `non_tectonic_type` (quarry blasts, explosions) from the feed and FDSN; 13 HANS rows `no_notice_issued`, 2 `region_level_notice`, 4 notices `missing_id_or_time`. One `unknown_source` row (the `usgs_hans_volcanoes` reference file) was a pipeline bug, fixed on 2026-11-19; exclude it or report it as such.
* Outlier flags in earthquakes (`is_outlier_*`), 27 % depth outliers in the Workshop 2 sample (Alaska slab events, Hawaii ~30 km events).
* Versions vs distinct ids (section 2) as the duplicate KPI; 708 VAN/VONA pairs share an identifier (key is id + type).
* Text length per source and period: `notices.text_length` / `summary_length` (mean 2,781 / 416 characters).
* Ingestion frequency: `ingestion_run` in the database (expected hourly runs vs actual), or Bronze file timestamps.
* The notebook `notebooks/03_data_quality_findings.ipynb` uses pandas on the same folders.

**Part 3 (storytelling).**

* Use `summary_clean` / `summary_tokens` for the activity tone: one paragraph per notice written by the observatory, comparable across 16 years (0.2 % missing). `tokens` covers the whole body.
* `notice_type` matters: daily updates (DU, 59 %) repeat the same situation every day; weekly updates (WU) give one view per week per observatory. Decide whether a trend counts every notice or one per volcano per week.
* `alert_level_ord` is the official level written in the notice; compare the tone with it (does an escalating tone come before a raised level?).
* Earthquakes vs baseline (US3): weekly counts per `nearest_vnum` from `earthquakes`, percentile against the same volcano's 2010-present weeks.
* VADER is not installed. If you use it, add `vaderSentiment` to `requirements.txt` and `airflow/requirements.txt` and run `docker compose up -d --build`. Its lexicon is made for social media, which is why the plan compares it with a small volcano lexicon (escalation words such as *increase, eruption, ash, unrest* vs de-escalation words such as *decrease, quiet, background, paused*).
* After changing text rules in Silver, rebuild that source with `scripts/reprocess_silver_source.py` (DAG paused).

The same data is in PostgreSQL (`volcano_notice`, `earthquake`, `eruption`, `volcano_alert`...) for queries in pgAdmin, but the Gold layer must be computed with PySpark from Silver.
