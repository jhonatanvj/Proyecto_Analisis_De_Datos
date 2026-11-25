# Workshop 4 integration adjustments

Changes made in Workshop 4 so that the dashboards show accurate and complete data. They are kept apart from the Workshop 3 pipeline improvements ([`../workshop_3/pipeline_improvements.md`](../workshop_3/pipeline_improvements.md)), as the workshop asks.

## 1. Silver `eruptions`: end date and continuing eruptions

**Found:** the Volcano Activity Indicator gives H = 1 to a volcano whose eruption is still going on (Workshop 1 design), but Silver kept only the start of each eruption. The GVP Bronze file does have the end (`EndDateYear/Month/Day`), and GVP marks an eruption that was still going on at its last update with `EndDateYearModifier = ">"` (for example Kilauea since 2024-12-23 and Great Sitkin since 2021-05-25, both "> 2026-03-25").

**Change:** three new columns, filled by `eruptions_from_gvp` in `src/volcano_pipeline/silver/transform.py`:

| Column | Type | Meaning |
|---|---|---|
| `end_time` | timestamp (UTC, µs) | end of the eruption, first day of the known period; null when unknown |
| `end_precision` | category | `day` / `month` / `year` / `unknown`, like `time_precision` for the start |
| `is_continuing` | bool | `true` when GVP wrote `>` before the end year |

An end date alone does not mean "ongoing": 6,602 of the 11,089 GVP eruptions have no end date, almost all of them old eruptions whose end is simply unknown. Only the `>` marker is read as continuing.

## 2. Silver `volcanoes`: observatory and region

**Found:** the VAI ranking must be filterable by region (user story US6: Alaska, Hawaii, Cascades...), but Silver `volcanoes` had no region. The HANS volcano list (`usgs_hans_volcanoes`, already ingested as a reference source) gives the USGS observatory and region of 153 of the 165 US volcanoes; GVP gives a subregion for all of them.

**Change:** three new columns, filled by `volcanoes_from_gvp` with the HANS list from the Silver run context:

| Column | Source | Example |
|---|---|---|
| `subregion` | GVP `Subregion` | `Aleutian Ridge Volcanic Arc` |
| `observatory` | HANS `obs_abbr` | `avo`, `hvo`, `cvo`, `calvo`, `yvo`, `nmi` |
| `usgs_region` | HANS `region` | `Alaska - Aleutians` |

The label shown in the dashboard is the observatory group (`config/sources.yaml`, `indicator.regions.observatories`), or the GVP subregion mapped to the same groups for the 12 volcanoes HANS does not list (`indicator.regions.subregions`: Mariana seamounts, Samoan seamounts, Gorda / Juan de Fuca ridge segments, Stepovak Bay). All 165 volcanoes get one of six regions; none falls into "Other".

## 3. Gold `storytelling`: four indicator sections

`src/volcano_pipeline/gold/indicator.py` adds `vai_current`, `vai_weekly`, `volcano_map` and `eruption_history` to the storytelling file (rules in [`vai_indicator.md`](vai_indicator.md)). `build_storytelling` calls it once; the Gold contract is unchanged.

One problem was found while testing it: GVP eruptions go back to year 1, and pandas cannot hold timestamps before 1677 in nanoseconds, so collecting the eruptions from Spark to pandas failed (`OutOfBoundsDatetime`). Dropping old eruptions was not an option, because a volcano that last erupted in 1510 has an eruptive component close to 0 but *available*, which changes how the weights are shared. The eruption times now leave Spark as seconds since 1970 (`test_eruption_before_1677_still_counts`).

The code review found a second one: GVP day-precision dates fall on 00:00 UTC, and the weekly eruptive component counted an eruption starting on a Monday also in the week before (9 of the 47 US eruptions since 2010). Weeks are now half-open periods, Monday 00:00 included, next Monday excluded (`test_eruption_starting_on_a_monday_does_not_reach_the_week_before`).

## 4. Gold reads each Silver folder with `mergeSchema`

`spark.read.parquet(folder)` takes the schema of one file. After a Silver column is added, a folder holds files with and without it, and the new column could silently disappear from Gold. `read_topic` in `gold/silver_reader.py` now reads with `mergeSchema` (`test_reader_merges_old_and_new_silver_schemas`).

## How to apply them to an existing data lake

The new Silver columns appear only in files written after the change. Silver writes a record again only when its source content changes, so the existing volcanoes and eruptions are **not** rewritten by later runs: rebuild the two GVP topics once, by hand. This is required on every data lake that existed before this change (Gold reads each topic folder with `mergeSchema`, so old and new files can sit together, but the old rows would keep the new columns empty and Kilauea and Great Sitkin would lose their continuing eruption):

```bash
docker compose exec airflow-scheduler airflow dags pause silver_processing
python scripts/reprocess_silver_source.py gvp_volcanoes
python scripts/reprocess_silver_source.py gvp_eruptions
docker compose exec airflow-scheduler airflow dags unpause silver_processing
docker compose exec airflow-scheduler airflow dags test gold_processing     # or trigger it in the UI
```

A fresh clone needs nothing extra: the first Silver run already writes the new columns.
