"""Storytelling summary: aggregations behind the functional-user dashboard (Workshop 3, Part 3).

build_storytelling() receives the Silver topics read by silver_reader and returns rows in the Gold
contract (see contract.py). Each aggregation is one small function mapped to a user story from
Workshop 1 (US1-US8), returning a DataFrame with section, metric, value (plus dimension / period /
unit when useful).

Current aggregations:
* earthquakes_weekly: earthquakes per week near US volcanoes (US2/US3; example of the pattern).
"""
from __future__ import annotations

from datetime import datetime


def earthquakes_weekly(spark, silver: dict):
    from pyspark.sql import functions as F

    eq = silver.get("earthquakes")
    if eq is None:
        return None
    latest = eq.dropDuplicates(["record_id"])   # the feed repeats records within its 7-day window
    return (latest.where(F.col("nearest_vnum").isNotNull())
            .groupBy(F.date_trunc("week", "event_time").alias("period"))
            .agg(F.count("*").cast("double").alias("value"))
            .select(F.lit("earthquakes_weekly").alias("section"), F.lit("earthquakes").alias("metric"),
                    "period", "value", F.lit("records").alias("unit")))


AGGREGATIONS = [earthquakes_weekly]


def build_storytelling(spark, silver: dict, generated: datetime):
    from .contract import conform, union_all

    parts = [agg(spark, silver) for agg in AGGREGATIONS]
    return union_all(spark, [conform(p, generated) for p in parts if p is not None])
