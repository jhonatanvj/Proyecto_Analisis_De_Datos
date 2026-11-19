"""Governance summary: data-quality KPIs of the Silver layer (Workshop 3, Part 2).

build_governance() receives the Silver topics read by silver_reader and returns rows in the Gold
contract (see contract.py). Each KPI is one small function returning a DataFrame with section,
metric, value (plus dimension / period / unit when useful), so a KPI can be added, tested and
explained on its own. Every KPI must be justified by a finding in the Silver data
(notebooks/03_data_quality_findings.ipynb).

Current KPIs:
* volume_total: records per Silver topic (example of the pattern).
"""
from __future__ import annotations

from datetime import datetime


def volume_total(spark, silver: dict):
    rows = [(topic, float(df.count())) for topic, df in silver.items() if df is not None]
    return spark.createDataFrame(rows, "dimension string, value double").selectExpr(
        "'volume_total' AS section", "'records' AS metric", "dimension", "value", "'records' AS unit")


KPIS = [volume_total]


def build_governance(spark, silver: dict, generated: datetime):
    from .contract import conform, union_all

    return union_all(spark, [conform(kpi(spark, silver), generated) for kpi in KPIS])
