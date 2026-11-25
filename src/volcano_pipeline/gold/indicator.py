"""Volcano Activity Indicator (VAI) sections of the storytelling Gold summary (Workshop 4).

    vai_current       per volcano: VAI score and level, the three components (null = not available,
                      US8), how many were used, earthquakes in the window and their percentile
    vai_weekly        per volcano with a seismic baseline and week since history_start: VAI from S
                      and H only (HANS keeps no alert history), plus both components (US7)
    volcano_map       per volcano: latitude, longitude, alert level and colour code ordinals (US1);
                      dimension = "vnum|name|region|observatory"
    eruption_history  per eruption since 1900: VEI and whether it is continuing, at its start date (US5)

The heavy part runs in Spark: the ~300k earthquakes are counted per volcano and week on a grid that
includes the weeks with no earthquake, and the percentile of every count among the volcano's own
weeks comes from a window over the count distribution. The results are small (one row per volcano,
or per volcano and week), so they are collected and the rules of vai_rules.py are applied in Python,
then turned back into a Spark DataFrame in the Gold contract.

The baseline is every complete week from history_start to the week before the run; the current week
is left out because it is not over. The weekly history compares each week with the whole baseline
(in-sample), which the Workshop 4 report states as a limitation.
"""
from __future__ import annotations

import math
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd

from .vai_rules import (COMPONENTS, combine, eruptive_component, level, region_label,
                        reported_component, seismic_component)

ALERT_ORD = {"NORMAL": 1, "ADVISORY": 2, "WATCH": 3, "WARNING": 4}
COLOR_ORD = {"GREEN": 1, "YELLOW": 2, "ORANGE": 3, "RED": 4}
ERUPTION_HISTORY_FROM = date(1900, 1, 1)
SECONDS_PER_YEAR = 365.25 * 86400


def _weeks(history_start: str, generated: datetime) -> list[date]:
    """Monday of every complete week from history_start to the week before the run."""
    first = date.fromisoformat(history_start)
    run_day = generated.astimezone(timezone.utc).date()
    current_monday = run_day - timedelta(days=run_day.weekday())
    count = (current_monday - first).days // 7
    return [first + timedelta(weeks=i) for i in range(count)]


def _columns(df, wanted: list[str]):
    """Select the wanted columns, null for the ones an older Silver file does not have."""
    from pyspark.sql import functions as F

    return df.select(*[F.col(c) if c in df.columns else F.lit(None).alias(c) for c in wanted])


def _volcanoes(silver: dict) -> pd.DataFrame:
    from .silver_reader import latest

    cols = ["vnum", "name", "latitude", "longitude", "subregion", "observatory"]
    vol = _columns(latest(silver["volcanoes"]), cols).toPandas()
    vol["vnum"] = vol["vnum"].astype("int64")
    return vol


def _latest_alerts(silver: dict) -> pd.DataFrame:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    from .silver_reader import latest

    notices = silver.get("alert_notices")
    if notices is None:
        return pd.DataFrame(columns=["vnum", "alert_level", "color_code"])
    w = Window.partitionBy("vnum").orderBy(F.col("sent_time").desc())
    out = (latest(notices).where(F.col("vnum").isNotNull())
           .withColumn("_rn", F.row_number().over(w)).where("_rn = 1")
           .select(F.col("vnum").cast("long"), "alert_level", "color_code").toPandas())
    return out


def _eruptions(silver: dict) -> pd.DataFrame:
    """Dated eruptions (start known; BCE / undated ones are dropped) with start and end as seconds
    since 1970. GVP dates go back to year 1, outside pandas' nanosecond timestamps (1677-2262), so
    the times leave Spark as numbers; start_date (a plain date) is kept for the eruption history."""
    from pyspark.sql import functions as F

    from .silver_reader import latest

    eruptions = silver.get("eruptions")
    cols = ["vnum", "start_s", "end_s", "start_date", "is_continuing", "vei"]
    if eruptions is None:
        return pd.DataFrame(columns=cols)
    df = (_columns(latest(eruptions), ["vnum", "start_time", "end_time", "is_continuing", "vei"])
          .where(F.col("start_time").isNotNull())
          .select(F.col("vnum").cast("long"),
                  F.col("start_time").cast("double").alias("start_s"),
                  F.col("end_time").cast("double").alias("end_s"),
                  F.to_date("start_time").alias("start_date"),
                  F.coalesce(F.col("is_continuing").cast("boolean"), F.lit(False)).alias("is_continuing"),
                  F.col("vei").cast("double"))
          .toPandas())
    return df


def _seismic(spark, silver: dict, weeks: list[date], generated: datetime, window_days: int):
    """Per volcano: earthquakes in the window and n_below / n_equal / n_weeks / baseline events;
    per volcano and week: the same numbers for the week's own count (for the weekly history)."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    from .silver_reader import latest

    empty_current = pd.DataFrame(columns=["vnum", "window_count", "n_below", "n_equal", "n_weeks",
                                          "baseline_events"])
    empty_weekly = pd.DataFrame(columns=["vnum", "week", "n_below", "n_equal", "n_weeks", "baseline_events"])
    earthquakes = silver.get("earthquakes")
    if earthquakes is None or not weeks:
        return empty_current, empty_weekly

    start = datetime.combine(weeks[0], datetime.min.time(), tzinfo=timezone.utc)
    end = start + timedelta(weeks=len(weeks))                   # Monday of the current week
    eq = (latest(earthquakes)
          .where(F.col("nearest_vnum").isNotNull() & F.col("event_time").isNotNull())
          .select(F.col("nearest_vnum").cast("long").alias("vnum"), "event_time"))

    per_week = (eq.where((F.col("event_time") >= F.lit(start)) & (F.col("event_time") < F.lit(end)))
                .groupBy("vnum", F.to_date(F.date_trunc("week", "event_time")).alias("week"))
                .agg(F.count("*").alias("n")))
    week_df = spark.createDataFrame([(w,) for w in weeks], "week date")
    # every volcano with a baseline earthquake gets every week; weeks without one count as 0
    grid = (per_week.select("vnum").distinct().crossJoin(week_df)
            .join(per_week, ["vnum", "week"], "left").fillna(0, subset=["n"]))
    totals = grid.groupBy("vnum").agg(F.count("*").alias("n_weeks"), F.sum("n").alias("baseline_events"))

    window = (eq.where((F.col("event_time") > F.lit(generated - timedelta(days=window_days)))
                       & (F.col("event_time") <= F.lit(generated)))
              .groupBy("vnum").agg(F.count("*").alias("window_count")))
    current = (grid.join(window, "vnum", "left").fillna(0, subset=["window_count"])
               .groupBy("vnum", "window_count")
               .agg(F.sum(F.when(F.col("n") < F.col("window_count"), 1).otherwise(0)).alias("n_below"),
                    F.sum(F.when(F.col("n") == F.col("window_count"), 1).otherwise(0)).alias("n_equal"))
               .join(totals, "vnum"))
    # also volcanoes with earthquakes in the window but none in the baseline (no S, but a count)
    window_only = window.join(totals, "vnum", "left_anti").select(
        "vnum", "window_count", F.lit(0).alias("n_below"), F.lit(0).alias("n_equal"),
        F.lit(0).alias("n_weeks"), F.lit(0).alias("baseline_events"))
    current = current.unionByName(window_only)

    # weekly: rank of each week's count within the volcano's distribution of weekly counts
    dist = grid.groupBy("vnum", "n").agg(F.count("*").alias("n_equal"))
    cum = Window.partitionBy("vnum").orderBy("n").rowsBetween(Window.unboundedPreceding, 0)
    dist = dist.withColumn("n_below", F.sum("n_equal").over(cum) - F.col("n_equal"))
    weekly = (grid.join(dist, ["vnum", "n"]).join(totals, "vnum")
              .select("vnum", "week", "n_below", "n_equal", "n_weeks", "baseline_events"))
    return current.toPandas(), weekly.toPandas()


def _eruptive(eruptions: pd.DataFrame, starts: np.ndarray, ends: np.ndarray, decay: float) -> list[float | None]:
    """H for half-open periods [starts, ends) in seconds since 1970: one per week for the history,
    and [now, now + 1 s) for the current value.

    A period is inside an eruption when the eruption started before the period ended and was still
    going on when the period started: continuing, or ended on or after that start. An eruption with
    no known end only covers its start. Otherwise H decays from the end (or start, when the end is
    unknown) of the last eruption before the end of the period; None before the first dated eruption.
    The end is exclusive: GVP day-precision dates fall on 00:00 UTC, so an eruption starting on a
    Monday belongs to that week, not to the week before.
    """
    if eruptions.empty:
        return [None] * len(starts)
    inside = np.zeros(len(starts), dtype=bool)
    for e in eruptions.itertuples(index=False):
        if e.is_continuing:
            still_on = np.ones(len(starts), dtype=bool)
        elif pd.notna(e.end_s):
            still_on = starts <= e.end_s
        else:
            still_on = starts <= e.start_s
        inside |= (e.start_s < ends) & still_on
    refs = np.sort(eruptions["end_s"].fillna(eruptions["start_s"]).to_numpy(dtype=float))
    last = np.searchsorted(refs, ends, side="left") - 1       # last reference strictly before the end
    out: list[float | None] = []
    for i, k in enumerate(last):
        if inside[i]:
            out.append(1.0)
        elif k < 0:
            out.append(None)
        else:
            out.append(eruptive_component((ends[i] - refs[k]) / SECONDS_PER_YEAR, False, decay))
    return out


def _text(value) -> str | None:
    return None if value is None or (isinstance(value, float) and math.isnan(value)) else str(value)


def _clean(value):
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    return float(value)


def _row(section, metric, dimension, value, unit, period=None):
    return (section, metric, dimension, period, _clean(value), unit)


def build_indicator(spark, silver: dict, generated: datetime, cfg: dict):
    """The four VAI sections as one DataFrame (section, metric, dimension, period, value, unit)."""
    if silver.get("volcanoes") is None:
        return None
    generated = generated.astimezone(timezone.utc)
    now = np.array([generated.timestamp()])
    now_end = now + 1.0                                        # the current moment as [now, now + 1 s)
    weights, levels, decay = cfg["weights"], cfg["levels"], cfg["eruptive_decay_years"]
    min_events = cfg["seismic_min_baseline_events"]

    vol = _volcanoes(silver)
    alerts = _latest_alerts(silver).set_index("vnum")
    eruptions = _eruptions(silver)
    by_volcano = {v: g for v, g in eruptions.groupby("vnum")}
    weeks = _weeks(cfg["history_start"], generated)
    current, weekly = _seismic(spark, silver, weeks, generated, cfg["seismic_window_days"])
    current = current.set_index("vnum")
    no_eruptions = eruptions.iloc[0:0]

    rows = []
    for v in vol.itertuples(index=False):
        vnum, dim = int(v.vnum), str(int(v.vnum))
        alert = alerts.loc[vnum] if vnum in alerts.index else None
        alert_level = None if alert is None else alert["alert_level"]
        color_code = None if alert is None else alert["color_code"]

        observatory = _text(v.observatory)
        name = (_text(v.name) or str(vnum)).replace("|", "/")
        region = region_label(observatory, _text(v.subregion), cfg["regions"])
        map_dim = f"{vnum}|{name}|{region}|{observatory or ''}"
        rows += [_row("volcano_map", "latitude", map_dim, v.latitude, "deg"),
                 _row("volcano_map", "longitude", map_dim, v.longitude, "deg"),
                 _row("volcano_map", "alert_level_ord", map_dim, ALERT_ORD.get(str(alert_level).upper()), "level"),
                 _row("volcano_map", "color_code_ord", map_dim, COLOR_ORD.get(str(color_code).upper()), "level")]

        seis = current.loc[vnum] if vnum in current.index else None
        s = p = None
        window_count = 0
        if seis is not None:
            window_count = int(seis["window_count"])
            s = seismic_component(int(seis["n_below"]), int(seis["n_equal"]), int(seis["n_weeks"]),
                                  int(seis["baseline_events"]), min_events)
            if s is not None:
                p = (int(seis["n_below"]) + 0.5 * int(seis["n_equal"])) / int(seis["n_weeks"])
        comps = {"reported": reported_component(alert_level, cfg["alert_level_scores"]),
                 "seismic": s,
                 "eruptive": _eruptive(by_volcano.get(vnum, no_eruptions), now, now_end, decay)[0]}
        score, used = combine(comps, weights)
        if score is None:
            continue
        rows += [_row("vai_current", "vai_score", dim, score, "score"),
                 _row("vai_current", "level_ord", dim, level(score, levels)[1], "level"),
                 _row("vai_current", "components_used", dim, len(used), "count"),
                 _row("vai_current", "earthquakes_window", dim, window_count, "count"),
                 _row("vai_current", "baseline_percentile", dim, p, "percentile")]
        rows += [_row("vai_current", c, dim, comps[c], "component") for c in COMPONENTS]

    for vnum, g in weekly.groupby("vnum"):
        if int(g["baseline_events"].iloc[0]) < min_events:
            continue
        dim = str(int(vnum))
        g = g.sort_values("week")
        week_s = np.array([datetime.combine(w, datetime.min.time(), tzinfo=timezone.utc).timestamp()
                           for w in g["week"]])
        hs = _eruptive(by_volcano.get(int(vnum), no_eruptions), week_s, week_s + 7 * 86400, decay)
        for w, h in zip(g.itertuples(index=False), hs):
            s = seismic_component(int(w.n_below), int(w.n_equal), int(w.n_weeks), int(w.baseline_events), min_events)
            score, _ = combine({"reported": None, "seismic": s, "eruptive": h}, weights)
            rows += [_row("vai_weekly", "vai_score", dim, score, "score", w.week),
                     _row("vai_weekly", "seismic", dim, s, "component", w.week),
                     _row("vai_weekly", "eruptive", dim, h, "component", w.week)]

    for e in eruptions.itertuples(index=False):
        if e.start_date < ERUPTION_HISTORY_FROM:
            continue
        dim, start = str(int(e.vnum)), e.start_date
        rows += [_row("eruption_history", "vei", dim, e.vei, "VEI", start),
                 _row("eruption_history", "continuing", dim, 1.0 if e.is_continuing else 0.0, "flag", start)]

    schema = "section string, metric string, dimension string, period date, value double, unit string"
    return spark.createDataFrame(rows, schema)
