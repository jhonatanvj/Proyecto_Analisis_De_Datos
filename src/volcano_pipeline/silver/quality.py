"""Outlier flags. At volcanoes, swarms and very shallow events are the signal, so outliers are
flagged for the analyst and never dropped."""
from __future__ import annotations

import pandas as pd


def iqr_outliers(s: pd.Series, k: float = 1.5) -> pd.Series:
    values = pd.to_numeric(s, errors="coerce")
    q1, q3 = values.quantile(0.25), values.quantile(0.75)
    iqr = q3 - q1
    flags = (values < q1 - k * iqr) | (values > q3 + k * iqr)
    return flags.fillna(False).astype(bool)


def robust_z_outliers(s: pd.Series, threshold: float = 3.5) -> pd.Series:
    """Modified z-score (Iglewicz and Hoaglin): 0.6745 * (x - median) / MAD."""
    values = pd.to_numeric(s, errors="coerce")
    median = values.median()
    mad = (values - median).abs().median()
    if not mad or pd.isna(mad):
        return pd.Series(False, index=s.index)
    z = 0.6745 * (values - median) / mad
    return (z.abs() > threshold).fillna(False).astype(bool)
