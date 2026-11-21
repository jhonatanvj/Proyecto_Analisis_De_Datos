"""Target schema of every Silver topic. Files of the same topic always share it, so PySpark
can later read the whole folder as one DataFrame (Workshop 3)."""
from __future__ import annotations

from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pyarrow as pa

TS = pa.timestamp("ns", tz="UTC")
# GVP eruptions go back centuries; nanosecond timestamps stop at 1677, microseconds do not
TS_US = pa.timestamp("us", tz="UTC")
CAT = pa.dictionary(pa.int32(), pa.string())

LINEAGE = ["bronze_file", "bronze_sha256", "processed_utc"]
_LINEAGE_FIELDS = [("bronze_file", pa.string()), ("bronze_sha256", pa.string()), ("processed_utc", TS)]
_ID_FIELDS = [("record_id", pa.string()), ("content_hash", pa.string())]

KEYWORD_COLUMNS = ["kw_eruption", "kw_ash", "kw_lava", "kw_seismicity", "kw_deformation", "kw_gas", "kw_plume"]


def _schema(fields: list[tuple[str, pa.DataType]]) -> pa.Schema:
    return pa.schema(_ID_FIELDS + fields + _LINEAGE_FIELDS)


SCHEMAS: dict[str, pa.Schema] = {
    "earthquakes": _schema([
        ("event_time", TS), ("updated_time", TS),
        ("latitude", pa.float64()), ("longitude", pa.float64()), ("depth_km", pa.float64()),
        ("mag", pa.float64()), ("mag_type", CAT), ("network", CAT), ("review_status", CAT),
        ("place", pa.string()), ("nearest_vnum", pa.int64()), ("distance_km", pa.float64()),
        ("mag_missing", pa.bool_()),
        ("is_outlier_mag_iqr", pa.bool_()), ("is_outlier_mag_z", pa.bool_()),
        ("is_outlier_depth_iqr", pa.bool_()), ("is_outlier_depth_z", pa.bool_()),
    ]),
    "alert_notices": _schema([
        ("vnum", pa.int64()), ("volcano_name", pa.string()), ("observatory", CAT),
        ("alert_level", CAT), ("alert_level_ord", pa.int64()), ("color_code", CAT),
        ("notice_identifier", pa.string()), ("sent_time", TS),
        ("latitude", pa.float64()), ("longitude", pa.float64()), ("coords_missing", pa.bool_()),
    ]),
    "volcanoes": _schema([
        ("vnum", pa.int64()), ("name", pa.string()), ("country", pa.string()),
        ("primary_type", pa.string()), ("last_eruption_year", pa.int64()),
        ("elevation_m", pa.float64()), ("latitude", pa.float64()), ("longitude", pa.float64()),
    ]),
    "eruptions": _schema([
        ("eruption_number", pa.int64()), ("vnum", pa.int64()), ("start_time", TS_US),
        ("time_precision", CAT), ("start_year", pa.int64()), ("vei", pa.int64()),
        ("vei_missing", pa.bool_()), ("activity_type", CAT),
        ("latitude", pa.float64()), ("longitude", pa.float64()),
    ]),
    "volcano_updates": _schema([
        ("record_type", CAT), ("observatory", CAT), ("volcano_name", pa.string()),
        ("vnum", pa.int64()), ("alert_level", CAT), ("color_code", CAT), ("as_of_time", TS),
        ("issued_raw", pa.string()), ("notice_number", pa.string()), ("notice_url", pa.string()),
        ("text_clean", pa.string()), ("tokens", pa.list_(pa.string())), ("token_count", pa.int64()),
    ] + [(k, pa.bool_()) for k in KEYWORD_COLUMNS]),
    # HANS notice texts (usgs_hans_notices), the text corpus of the storytelling summary
    "notices": _schema([
        ("notice_identifier", pa.string()), ("sent_time", TS), ("observatory", CAT), ("notice_type", CAT),
        ("volcano_codes", pa.string()), ("vnums", pa.list_(pa.int64())), ("vnum", pa.int64()),
        ("volcano_name", pa.string()), ("region", CAT), ("is_region_wide", pa.bool_()),
        ("alert_level", CAT), ("alert_level_ord", pa.int64()), ("color_code", CAT),
        ("summary_clean", pa.string()), ("summary_tokens", pa.list_(pa.string())),
        ("text_clean", pa.string()), ("tokens", pa.list_(pa.string())), ("token_count", pa.int64()),
        ("text_length", pa.int64()), ("summary_length", pa.int64()), ("notice_url", pa.string()),
    ] + [(k, pa.bool_()) for k in KEYWORD_COLUMNS]),
}

QUARANTINE_SCHEMA = pa.schema([
    ("source_id", pa.string()), ("reason", pa.string()), ("payload", pa.string()),
    ("bronze_file", pa.string()), ("processed_utc", TS),
])


def _to_us_utc(value) -> np.datetime64:
    if value is None or value is pd.NaT or (isinstance(value, float) and np.isnan(value)):
        return np.datetime64("NaT", "us")
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if isinstance(value, datetime):     # also covers pd.Timestamp
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return np.datetime64(value, "us")
    return np.datetime64("NaT", "us")


def _cast(series: pd.Series, typ: pa.DataType) -> pd.Series:
    if pa.types.is_timestamp(typ) and typ.unit == "us":
        values = np.array([_to_us_utc(v) for v in series], dtype="datetime64[us]")
        return pd.Series(values, index=series.index).dt.tz_localize("UTC")
    if pa.types.is_timestamp(typ):
        return pd.to_datetime(series, utc=True, errors="coerce").astype("datetime64[ns, UTC]")
    if pa.types.is_floating(typ):
        return pd.to_numeric(series, errors="coerce").astype("float64")
    if pa.types.is_integer(typ):
        return pd.to_numeric(series, errors="coerce").round().astype("Int64")
    if pa.types.is_boolean(typ):
        return series.fillna(False).astype(bool)
    if pa.types.is_dictionary(typ):
        return series.astype("string").astype("category")
    if pa.types.is_list(typ):
        return series.apply(lambda v: list(v) if isinstance(v, (list, tuple)) else [])
    return series.astype("string").astype(object).where(series.notna(), None)


def enforce(df: pd.DataFrame, topic: str, schema: pa.Schema | None = None) -> pd.DataFrame:
    """Keep exactly the schema columns, in order, adding missing ones as nulls and casting the rest."""
    schema = schema or SCHEMAS[topic]
    out = pd.DataFrame(index=df.index)
    for field in schema:
        col = df[field.name] if field.name in df.columns else pd.Series([None] * len(df), index=df.index, dtype=object)
        out[field.name] = _cast(col, field.type)
    out = out.reset_index(drop=True)
    out.attrs = dict(df.attrs)
    return out


def to_table(df: pd.DataFrame, schema: pa.Schema) -> pa.Table:
    return pa.Table.from_pandas(df, schema=schema, preserve_index=False)
