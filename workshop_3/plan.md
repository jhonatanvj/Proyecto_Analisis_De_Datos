# Workshop 3 plan: work split for Team 6

Status: agreed 2026-11-19. Workshop 3 document: *Silver-to-Gold Pipeline, Governance KPIs, and Storytelling Definition* (the final grade also uses the CourseProject rubric, so two items from its Workshop 3 are included in Part 1).

## What the workshop asks for

1. Improvements to the Bronze and Silver pipelines based on what we observed, with a before/after comparison.
2. A Gold DAG `airflow/dags/gold_processing_dag.py` (scheduled, e.g. `@weekly`) that reads the Silver Parquet folder with PySpark in `local[*]` mode inside Docker and writes `datalake_gold/governance_YYYYMMDD_HHMMSS.parquet` and `datalake_gold/storytelling_YYYYMMDD_HHMMSS.parquet`.
3. Governance KPIs (null rate, volume, duplicates, schema compliance, outliers, text length, ingestion frequency), each justified by a real Silver finding.
4. Storytelling aggregations (sentiment, keywords, temporal trends, source comparison), each mapped to a Workshop 1 user story, plus the dashboard narrative.
5. A data-quality findings report backed by a pandas notebook.
6. Deliverables: notebooks (quality findings, Gold preview), `workshop_3/` with the report PDF, README update, Gold Parquet samples in the repo, screenshots of successful runs.

Workshop 4 will build two Dash apps that read **only** the Gold files: `dashboard/governance_app.py` (port 8050) and `dashboard/storytelling_app.py` (port 8051). Every Gold aggregation should map to one chart or KPI card.

## Where we start from

| Done | Missing |
|---|---|
| Bronze, Silver, Silver → PostgreSQL loader, `ingest.py`, 89 tests | Spark and Java in the Airflow image; `datalake_gold/` is empty |
| Silver outlier and missing-value flags, quarantine with reasons | Quality-findings notebook and KPIs in PySpark |
| Volcano Updates text with tokens and keyword flags | Text corpus is tiny (9 records); no sentiment score |
| | CourseProject W3: `event_group` is empty; FDSN incremental watermark (`updatedafter`) not used |

Two blockers to solve first:

* `spark.read.parquet("datalake_silver/")` would mix the five topics (different schemas) and the `_state/` files. Silver must move to one sub-folder per topic.
* "Sentiment" assumes public opinion. For volcanoes we adapt it to an **activity tone** of the USGS notices: a domain lexicon (escalation terms such as *increase, eruption, ash* vs. de-escalation terms such as *decrease, quiet, background*), compared with VADER as an experiment. Labels: escalating / stable / calming, coloured red / grey / green as Workshop 4 requires.

## Gold output contract

Both Gold files use the same long format, so Parts 2 and 3 never edit the same code and the Workshop 4 dashboards can filter one table:

| Column | Type | Meaning |
|---|---|---|
| `section` | string | Chart or card the row feeds, e.g. `null_rate`, `volume_weekly`, `tone_weekly`, `top_keywords` |
| `metric` | string | What is measured, e.g. `pct_null`, `records`, `mean_tone` |
| `dimension` | string | Breakdown value (field name, source, volcano, keyword, observatory); null if none |
| `period` | date | Start of the period (week or day); null for all-time values |
| `value` | double | The number |
| `unit` | string | `%`, `records`, `score`, `km`... |
| `generated_utc` | timestamp | When the Gold run produced the row |

## Split

Part 1 goes first because the other two depend on it. Parts 2 and 3 have the same shape (analysis → PySpark function → tests → report sections), do not depend on each other and can run in parallel on their own branches.

### Part 1 — Platform and pipeline improvements (Jhonatan)

1. Spark in Docker: Java + `pyspark` in the Airflow image, a `get_spark()` helper with documented memory settings, compose adjustments.
2. Silver with one sub-folder per topic, so `spark.read.parquet` reads one consistent schema; loader and tests updated.
3. Larger text corpus: full text of every HANS notice (`notice/getNotice`) plus the hourly Volcano Updates; URL/HTML noise removal and stop-word review.
4. Gold DAG skeleton (`@weekly`) with empty `build_governance(spark)` and `build_storytelling(spark)`, the timestamped Parquet writer and the output contract above.
5. CourseProject items: FDSN incremental watermark (`updatedafter`) and `event_group` matching rules.
6. Report sections: *Pipeline Improvements* (before/after) and *PySpark Implementation*.

Hand-off to Parts 2 and 3: a working Gold DAG that writes two (empty-but-valid) Parquet files, sample Silver data in the new layout and a short "how to add an aggregation" note.

### Part 2 — Data governance (Julián Pulido)

1. Notebook `notebooks/03_data_quality_findings.ipynb` (pandas): null rates, outliers, text lengths, duplicates, quarantine reasons, with tables and charts.
2. Choose the KPIs and justify each with a finding. Real examples to start from: 37 % of eruptions without VEI, 2,019 rejected records (mostly `non_tectonic_type`), 4 HANS notices without coordinates, 27 % depth outliers in the feed sample.
3. Implement `build_governance(spark)` in PySpark with pytest tests on small Parquet fixtures.
4. Report sections: *Data Quality Findings* and *Governance KPI Definition*.

### Part 3 — Storytelling (Juan Sebastián Rangel)

1. Write the narrative first: which US volcanoes show unusual activity, and is it building or fading?
2. Implement the activity tone (lexicon + VADER comparison) on the corpus from Part 1.
3. Implement `build_storytelling(spark)`: weekly tone, top keywords and bigrams, API (HANS) vs. scraping comparison, weekly earthquakes vs. each volcano's baseline (US3), alerts by observatory and region (US1, US6), with tests.
4. Notebook `notebooks/04_gold_preview.ipynb`, Gold samples in `workshop_3/samples/`, screenshots.
5. Report sections: *Storytelling Aggregations* and the narrative; assemble the final PDF from everyone's sections.

## Working rules

* One branch per part (`w3-platform`, `w3-governance`, `w3-storytelling`), merged into `main` through a pull request reviewed by another member.
* Code comments and documentation in English; no keys or `.env` in commits.
* `pytest -q` must pass before opening a pull request.
* Each member must be able to explain their part in the final defense (individual grade).
