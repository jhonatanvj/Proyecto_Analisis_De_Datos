"""Load Silver Parquet files into the project database.

Each Silver file is loaded in one transaction:

1. rows are copied (COPY) into a temporary staging table;
2. they are upserted into ``event`` + the domain table. A row that already exists is only
   rewritten when its ``content_hash`` changed, so loading the same data twice changes nothing;
3. rows that cannot be stored (no coordinates, volcano not in the catalogue) go to
   ``rejected_record`` with the reason;
4. one ``ingestion_run`` row per source in the file, the source watermark in
   ``ingestion_state`` and the file name in ``silver_load_file`` are written in the same
   transaction, so data and watermark always move together.

Files are loaded oldest first and, for the same timestamp, volcanoes before the tables that
reference them. A file already listed in ``silver_load_file`` is skipped. If a file fails, its
transaction is rolled back, a ``failed`` run is recorded and the loader stops, because later
files may depend on it; the next call retries from that file.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

TOPIC_ORDER = ["volcanoes", "eruptions", "earthquakes", "alert_notices", "volcano_updates", "quarantine"]
SILVER_NAME = re.compile(r"^(?P<topic>[a-z_]+?)_(?P<ts>\d{8}_\d{6})\.parquet$")
BRONZE_SOURCE = re.compile(r"^(?P<source>[a-z0-9_]+?)_\d{8}_\d{6}\.json$")
DEFAULT_SOURCE = {"volcanoes": "gvp_volcanoes", "eruptions": "gvp_eruptions", "earthquakes": "usgs_feed",
                  "alert_notices": "usgs_hans", "volcano_updates": "usgs_volcano_updates"}
COLOR_CODES = {"GREEN", "YELLOW", "ORANGE", "RED"}
LOCK_KEY = 6_2026_11   # pg_advisory_xact_lock key: one loader at a time (Airflow task vs ingest.py)

HANS_NOTICE_URL = "https://volcanoes.usgs.gov/hans2/view/notice/"
USGS_EVENT_URL = "https://earthquake.usgs.gov/earthquakes/eventpage/"
GVP_VOLCANO_URL = "https://volcano.si.edu/volcano.cfm?vn="


class SchemaMissing(RuntimeError):
    pass


# --------------------------------------------------------------------------- files and sources

def silver_files(silver_dir: Path) -> list[tuple[str, str, Path]]:
    """(topic, load name, path) for every Silver file, in load order.

    Topic files live in datalake_silver/<topic>/ (older runs wrote them to datalake_silver/
    directly; both are read). The load name of a topic file is its bare file name, so moving a
    file between the two layouts does not load it again; quarantine files are 'quarantine/<name>'.
    """
    found = []
    candidates = [(p, p.name) for p in silver_dir.glob("*.parquet")]
    candidates += [(p, p.name) for t in TOPIC_ORDER[:-1] for p in (silver_dir / t).glob(f"{t}_*.parquet")]
    candidates += [(p, f"quarantine/{p.name}") for p in (silver_dir / "quarantine").glob("rejected_*.parquet")]
    for path, rel in candidates:
        m = SILVER_NAME.match(path.name)
        if not m:
            continue
        topic = "quarantine" if m.group("topic") == "rejected" else m.group("topic")
        if topic in TOPIC_ORDER:
            found.append((m.group("ts"), TOPIC_ORDER.index(topic), topic, rel, path))
    return [(topic, rel, path) for _, _, topic, rel, path in sorted(found)]


def source_of(bronze_file: str | None, default: str | None = None) -> str | None:
    m = BRONZE_SOURCE.match(bronze_file or "")
    return m.group("source") if m else default


# --------------------------------------------------------------------------- row preparation
# Pure functions (no database): Silver DataFrame -> staging DataFrame + rejected rows.

@dataclass
class Prepared:
    stage: pd.DataFrame
    rejects: list[dict] = field(default_factory=list)


def _with_source(df: pd.DataFrame, topic: str) -> pd.DataFrame:
    df = df.drop_duplicates("record_id", keep="last").reset_index(drop=True).copy()
    df["source_id"] = [source_of(b, DEFAULT_SOURCE[topic]) for b in df["bronze_file"]]
    return df


def _reject_rows(df: pd.DataFrame, mask: pd.Series, reason: str) -> list[dict]:
    out = []
    for _, row in df[mask].iterrows():
        payload = {k: _jsonable(v) for k, v in row.items() if k not in ("bronze_sha256",)}
        out.append({"source_id": row["source_id"], "reason": reason, "payload": json.dumps(payload, default=str),
                    "bronze_file": row.get("bronze_file"), "processed_utc": row.get("processed_utc")})
    return out


def _jsonable(v):
    if isinstance(v, np.ndarray):
        return v.tolist()
    if isinstance(v, (list, dict)):
        return v
    if v is None or (np.isscalar(v) and pd.isna(v)):
        return None
    if isinstance(v, np.generic):
        return v.item()
    return v


def _missing_coords(df: pd.DataFrame) -> pd.Series:
    return df["latitude"].isna() | df["longitude"].isna()


def prep_volcanoes(df: pd.DataFrame) -> Prepared:
    df = _with_source(df, "volcanoes")
    bad = _missing_coords(df)
    stage = df[~bad].assign(elevation_m=df["elevation_m"].round().astype("Int64"))
    return Prepared(stage, _reject_rows(df, bad, "missing_coordinates"))


def prep_eruptions(df: pd.DataFrame) -> Prepared:
    df = _with_source(df, "eruptions")
    precision = df["time_precision"].astype("string")
    start = df["start_time"]
    df["start_month"] = start.dt.month.where(precision.isin(["day", "month"])).astype("Int64")
    df["start_day"] = start.dt.day.where(precision == "day").astype("Int64")
    return Prepared(df)     # coordinates missing -> taken from the volcano table on load


def prep_earthquakes(df: pd.DataFrame) -> Prepared:
    df = _with_source(df, "earthquakes")
    df["usgs_id"] = df["record_id"].str.removeprefix("usgs:")
    bad = _missing_coords(df)
    return Prepared(df[~bad], _reject_rows(df, bad, "missing_coordinates"))


def prep_alert_notices(df: pd.DataFrame) -> Prepared:
    df = _with_source(df, "alert_notices")
    color = df["color_code"].astype("string").str.upper()
    df["color_code"] = color.where(color.isin(COLOR_CODES))     # unknown wording -> NULL, not a failed load
    return Prepared(df)


def prep_volcano_updates(df: pd.DataFrame) -> Prepared:
    return Prepared(_with_source(df, "volcano_updates"))


def prep_quarantine(df: pd.DataFrame) -> Prepared:
    df = df.reset_index(drop=True).copy()
    df["source_id"] = [s if isinstance(s, str) and s else source_of(b) for s, b in zip(df["source_id"], df["bronze_file"])]
    df["payload"] = [_as_json(p) for p in df["payload"]]
    return Prepared(df)


def _as_json(payload) -> str:
    """Silver stores the payload as a JSON string; a few are plain text (unreadable files)."""
    if payload is None or (isinstance(payload, float) and np.isnan(payload)):
        return "null"
    try:
        json.loads(payload)
        return payload
    except (TypeError, ValueError):
        return json.dumps(str(payload))


PREPARE = {"volcanoes": prep_volcanoes, "eruptions": prep_eruptions, "earthquakes": prep_earthquakes,
           "alert_notices": prep_alert_notices, "volcano_updates": prep_volcano_updates,
           "quarantine": prep_quarantine}


def watermark_candidates(topic: str, stage: pd.DataFrame, cfg: dict) -> dict[str, tuple[str, str]]:
    """Per source: (kind, value). kind 'time' only moves forward; 'version' is replaced."""
    if stage.empty or topic == "quarantine":
        return {}
    if topic in ("volcanoes", "eruptions"):
        version = _source_cfg(cfg, DEFAULT_SOURCE[topic]).get("database_version")
        return {s: ("version", str(version)) for s in stage["source_id"].unique()} if version else {}
    column = {"earthquakes": "updated_time", "alert_notices": "sent_time", "volcano_updates": "as_of_time"}[topic]
    out = {}
    for source, part in stage.groupby("source_id"):
        latest = part[column].max()
        if pd.isna(latest):
            latest = part["processed_utc"].max()
        if not pd.isna(latest):
            out[source] = ("time", format_ts(latest))
    return out


def format_ts(ts) -> str:
    """Fixed-width UTC ISO string, so text comparison equals time comparison."""
    return pd.Timestamp(ts).tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _source_cfg(cfg: dict, source_id: str) -> dict:
    return next((s for s in cfg.get("sources", []) if s["source_id"] == source_id), {})


def extra_volcanoes(cfg: dict) -> list[dict]:
    return list(_source_cfg(cfg, "usgs_hans").get("extra_volcanoes") or [])


# --------------------------------------------------------------------------- COPY helpers

def py_values(series: pd.Series) -> list:
    """Column values as plain Python objects (psycopg does not adapt numpy scalars)."""
    if pd.api.types.is_datetime64_any_dtype(series):
        return [None if pd.isna(x) else x.to_pydatetime() for x in series]
    if pd.api.types.is_bool_dtype(series):
        return [None if pd.isna(x) else bool(x) for x in series]
    if pd.api.types.is_integer_dtype(series):
        return [None if pd.isna(x) else int(x) for x in series]
    if pd.api.types.is_float_dtype(series):
        return [None if pd.isna(x) else float(x) for x in series]
    out = []
    for x in series:
        if isinstance(x, (np.ndarray, tuple, list)):
            out.append([str(t) for t in x])
        elif x is None or (np.isscalar(x) and pd.isna(x)):
            out.append(None)
        else:
            out.append(x.item() if isinstance(x, np.generic) else x)
    return out


def copy_stage(cur, name: str, columns: list[tuple[str, str]], df: pd.DataFrame) -> int:
    cols = ", ".join(f"{c} {t}" for c, t in columns)
    cur.execute(f"CREATE TEMP TABLE {name} ({cols}) ON COMMIT DROP")
    if df.empty:
        return 0
    values = [py_values(df[c]) if c in df.columns else [None] * len(df) for c, _ in columns]
    with cur.copy(f"COPY {name} ({', '.join(c for c, _ in columns)}) FROM STDIN") as cp:
        cp.set_types([t for _, t in columns])
        for row in zip(*values):
            cp.write_row(row)
    return len(df)


LINEAGE_COLS = [("bronze_file", "text"), ("bronze_sha256", "text"), ("processed_utc", "timestamptz")]
STAGE_COLUMNS = {
    "volcanoes": [("record_id", "text"), ("content_hash", "text"), ("source_id", "text"), ("run_id", "int8"),
                  ("vnum", "int8"), ("name", "text"), ("country", "text"), ("primary_type", "text"),
                  ("last_eruption_year", "int8"), ("elevation_m", "int8"),
                  ("latitude", "float8"), ("longitude", "float8")] + LINEAGE_COLS,
    "eruptions": [("record_id", "text"), ("content_hash", "text"), ("source_id", "text"), ("run_id", "int8"),
                  ("eruption_number", "int8"), ("vnum", "int8"), ("start_time", "timestamptz"),
                  ("time_precision", "text"), ("start_year", "int8"), ("start_month", "int8"), ("start_day", "int8"),
                  ("vei", "int8"), ("vei_missing", "bool"), ("activity_type", "text"),
                  ("latitude", "float8"), ("longitude", "float8")] + LINEAGE_COLS,
    "earthquakes": [("record_id", "text"), ("content_hash", "text"), ("source_id", "text"), ("run_id", "int8"),
                    ("usgs_id", "text"), ("event_time", "timestamptz"), ("updated_time", "timestamptz"),
                    ("latitude", "float8"), ("longitude", "float8"), ("depth_km", "float8"), ("mag", "float8"),
                    ("mag_type", "text"), ("network", "text"), ("review_status", "text"), ("place", "text"),
                    ("nearest_vnum", "int8"), ("distance_km", "float8"), ("mag_missing", "bool"),
                    ("is_outlier_mag_iqr", "bool"), ("is_outlier_mag_z", "bool"),
                    ("is_outlier_depth_iqr", "bool"), ("is_outlier_depth_z", "bool")] + LINEAGE_COLS,
    "alert_notices": [("record_id", "text"), ("content_hash", "text"), ("source_id", "text"), ("run_id", "int8"),
                      ("vnum", "int8"), ("volcano_name", "text"), ("observatory", "text"),
                      ("alert_level", "text"), ("alert_level_ord", "int8"), ("color_code", "text"),
                      ("notice_identifier", "text"), ("sent_time", "timestamptz"),
                      ("latitude", "float8"), ("longitude", "float8"), ("coords_missing", "bool")] + LINEAGE_COLS,
    "volcano_updates": [("record_id", "text"), ("content_hash", "text"), ("source_id", "text"), ("run_id", "int8"),
                        ("record_type", "text"), ("observatory", "text"), ("volcano_name", "text"), ("vnum", "int8"),
                        ("alert_level", "text"), ("color_code", "text"), ("as_of_time", "timestamptz"),
                        ("issued_raw", "text"), ("notice_number", "text"), ("notice_url", "text"),
                        ("text_clean", "text"), ("tokens", "text[]"), ("token_count", "int8"),
                        ("kw_eruption", "bool"), ("kw_ash", "bool"), ("kw_lava", "bool"), ("kw_seismicity", "bool"),
                        ("kw_deformation", "bool"), ("kw_gas", "bool"), ("kw_plume", "bool")] + LINEAGE_COLS,
    "quarantine": [("source_id", "text"), ("run_id", "int8"), ("reason", "text"), ("payload", "text"),
                   ("bronze_file", "text"), ("processed_utc", "timestamptz")],
}
REJECT_COLUMNS = STAGE_COLUMNS["quarantine"]

# --------------------------------------------------------------------------- SQL per topic
# Every statement reads the staging table "stage". Statements that write domain rows end by
# filling the temp table "changed" (event_id/record_id, source_id, is_new) used for the counts.

_EVENT_SET = """
    source_id = EXCLUDED.source_id, event_time = EXCLUDED.event_time,
    time_precision = EXCLUDED.time_precision, latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude,
    severity = EXCLUDED.severity, severity_unit = EXCLUDED.severity_unit,
    raw_reference = EXCLUDED.raw_reference, content_hash = EXCLUDED.content_hash,
    last_seen_run = EXCLUDED.last_seen_run, updated_at = now(),
    bronze_file = EXCLUDED.bronze_file, bronze_sha256 = EXCLUDED.bronze_sha256,
    silver_processed_utc = EXCLUDED.silver_processed_utc
"""
_EVENT_COLS = """(event_id, source_id, event_type, event_time, time_precision, latitude, longitude,
    severity, severity_unit, raw_reference, content_hash, first_seen_run, last_seen_run,
    bronze_file, bronze_sha256, silver_processed_utc)"""


def _event_upsert(select_sql: str) -> str:
    return f"""
WITH up AS (
    INSERT INTO event {_EVENT_COLS}
    {select_sql}
    ON CONFLICT (event_id) DO UPDATE SET {_EVENT_SET}
    WHERE event.content_hash IS DISTINCT FROM EXCLUDED.content_hash
    RETURNING event_id, source_id, (xmax = 0) AS is_new)
INSERT INTO changed SELECT event_id, source_id, is_new FROM up"""


def _orphans(condition: str, reason: str) -> str:
    """Move staging rows whose volcano is unknown to rejected_record, then drop them from stage."""
    return f"""
WITH gone AS (DELETE FROM stage s WHERE {condition} RETURNING s.*)
INSERT INTO rejected_record (run_id, source_id, reason, payload, bronze_file, silver_processed_utc)
SELECT run_id, source_id, '{reason}', to_jsonb(gone) - 'run_id', bronze_file, processed_utc FROM gone"""


NO_VOLCANO = "NOT EXISTS (SELECT 1 FROM volcano v WHERE v.volcano_number = s.vnum)"

SQL = {
    "volcanoes": ["""
WITH up AS (
    INSERT INTO volcano (volcano_number, name, country, primary_type, elevation_m, last_eruption_year,
                         latitude, longitude, is_holocene, source_id, gvp_db_version, content_hash,
                         bronze_file, bronze_sha256, silver_processed_utc)
    SELECT vnum, name, country, primary_type, elevation_m, last_eruption_year, latitude, longitude, true,
           source_id, %(gvp_version)s, content_hash, bronze_file, bronze_sha256, processed_utc FROM stage
    ON CONFLICT (volcano_number) DO UPDATE SET
        name = EXCLUDED.name, country = EXCLUDED.country, primary_type = EXCLUDED.primary_type,
        elevation_m = EXCLUDED.elevation_m, last_eruption_year = EXCLUDED.last_eruption_year,
        latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude, is_holocene = true,
        source_id = EXCLUDED.source_id, gvp_db_version = EXCLUDED.gvp_db_version,
        content_hash = EXCLUDED.content_hash, bronze_file = EXCLUDED.bronze_file,
        bronze_sha256 = EXCLUDED.bronze_sha256, silver_processed_utc = EXCLUDED.silver_processed_utc
    WHERE volcano.content_hash IS DISTINCT FROM EXCLUDED.content_hash
       OR volcano.source_id IS DISTINCT FROM EXCLUDED.source_id
    RETURNING volcano_number::text, source_id, (xmax = 0) AS is_new)
INSERT INTO changed SELECT * FROM up"""],

    "eruptions": [
        _orphans(NO_VOLCANO, "volcano_not_in_catalog"),
        _event_upsert(f"""
    SELECT s.record_id, s.source_id, 'eruption', s.start_time, coalesce(s.time_precision, 'unknown'),
           coalesce(s.latitude, v.latitude), coalesce(s.longitude, v.longitude), s.vei, 'VEI',
           '{GVP_VOLCANO_URL}' || s.vnum, s.content_hash, s.run_id, s.run_id,
           s.bronze_file, s.bronze_sha256, s.processed_utc
    FROM stage s JOIN volcano v ON v.volcano_number = s.vnum"""),
        """
INSERT INTO eruption (event_id, eruption_number, volcano_number, activity_type, vei,
                      start_year, start_month, start_day, vei_missing)
SELECT s.record_id, s.eruption_number, s.vnum, s.activity_type, s.vei,
       s.start_year, s.start_month, s.start_day, s.vei_missing
FROM stage s JOIN changed c ON c.key = s.record_id
ON CONFLICT (event_id) DO UPDATE SET
    eruption_number = EXCLUDED.eruption_number, volcano_number = EXCLUDED.volcano_number,
    activity_type = EXCLUDED.activity_type, vei = EXCLUDED.vei, start_year = EXCLUDED.start_year,
    start_month = EXCLUDED.start_month, start_day = EXCLUDED.start_day, vei_missing = EXCLUDED.vei_missing"""],

    "earthquakes": [
        _event_upsert(f"""
    SELECT record_id, source_id, 'earthquake', event_time, 'second', latitude, longitude, mag,
           'magnitude (' || coalesce(mag_type, 'unknown') || ')', '{USGS_EVENT_URL}' || usgs_id,
           content_hash, run_id, run_id, bronze_file, bronze_sha256, processed_utc
    FROM stage"""),
        """
INSERT INTO earthquake (event_id, usgs_id, magnitude, mag_type, depth_km, place, review_status, network,
                        source_updated, nearest_vnum, distance_km, mag_missing, is_outlier_mag_iqr,
                        is_outlier_mag_z, is_outlier_depth_iqr, is_outlier_depth_z)
SELECT s.record_id, s.usgs_id, s.mag, s.mag_type, s.depth_km, s.place, s.review_status, s.network,
       s.updated_time, v.volcano_number, CASE WHEN v.volcano_number IS NOT NULL THEN s.distance_km END,
       s.mag_missing, s.is_outlier_mag_iqr, s.is_outlier_mag_z, s.is_outlier_depth_iqr, s.is_outlier_depth_z
FROM stage s JOIN changed c ON c.key = s.record_id
LEFT JOIN volcano v ON v.volcano_number = s.nearest_vnum
ON CONFLICT (event_id) DO UPDATE SET
    usgs_id = EXCLUDED.usgs_id, magnitude = EXCLUDED.magnitude, mag_type = EXCLUDED.mag_type,
    depth_km = EXCLUDED.depth_km, place = EXCLUDED.place, review_status = EXCLUDED.review_status,
    network = EXCLUDED.network, source_updated = EXCLUDED.source_updated,
    nearest_vnum = EXCLUDED.nearest_vnum, distance_km = EXCLUDED.distance_km,
    mag_missing = EXCLUDED.mag_missing, is_outlier_mag_iqr = EXCLUDED.is_outlier_mag_iqr,
    is_outlier_mag_z = EXCLUDED.is_outlier_mag_z, is_outlier_depth_iqr = EXCLUDED.is_outlier_depth_iqr,
    is_outlier_depth_z = EXCLUDED.is_outlier_depth_z""",
        """
INSERT INTO volcano_earthquake (volcano_number, event_id, distance_km)
SELECT v.volcano_number, s.record_id, s.distance_km
FROM stage s JOIN changed c ON c.key = s.record_id
JOIN volcano v ON v.volcano_number = s.nearest_vnum
WHERE s.distance_km IS NOT NULL
ON CONFLICT (volcano_number, event_id) DO UPDATE SET distance_km = EXCLUDED.distance_km"""],

    "alert_notices": [
        # monitored volcanoes missing from GVP (config usgs_hans.extra_volcanoes)
        """
INSERT INTO volcano (volcano_number, name, latitude, longitude, is_holocene, observatory, source_id, content_hash)
SELECT x.vnum, x.name, x.latitude, x.longitude, false, x.observatory, 'usgs_hans', x.content_hash
FROM extra_volcano x
WHERE EXISTS (SELECT 1 FROM stage s WHERE s.vnum = x.vnum)
ON CONFLICT (volcano_number) DO NOTHING""",
        _orphans(NO_VOLCANO, "volcano_not_in_catalog"),
        """
UPDATE volcano v SET observatory = s.observatory
FROM (SELECT DISTINCT ON (vnum) vnum, observatory FROM stage WHERE observatory IS NOT NULL
      ORDER BY vnum, sent_time DESC) s
WHERE v.volcano_number = s.vnum AND v.observatory IS DISTINCT FROM s.observatory""",
        _event_upsert(f"""
    SELECT s.record_id, s.source_id, 'alert_notice', s.sent_time, 'second',
           coalesce(s.latitude, v.latitude), coalesce(s.longitude, v.longitude),
           s.alert_level_ord, 'alert level 1-4', '{HANS_NOTICE_URL}' || s.notice_identifier,
           s.content_hash, s.run_id, s.run_id, s.bronze_file, s.bronze_sha256, s.processed_utc
    FROM stage s JOIN volcano v ON v.volcano_number = s.vnum"""),
        f"""
INSERT INTO volcano_alert (event_id, volcano_number, notice_identifier, alert_level, color_code,
                           previous_alert_level, previous_color_code, observatory, sent_utc, notice_url,
                           alert_level_ord, coords_missing)
SELECT s.record_id, s.vnum, s.notice_identifier, s.alert_level, s.color_code,
       prev.alert_level, prev.color_code, s.observatory, s.sent_time,
       '{HANS_NOTICE_URL}' || s.notice_identifier, s.alert_level_ord, s.coords_missing
FROM stage s JOIN changed c ON c.key = s.record_id
LEFT JOIN LATERAL (SELECT a.alert_level, a.color_code FROM volcano_alert a
                   WHERE a.volcano_number = s.vnum AND a.sent_utc < s.sent_time
                   ORDER BY a.sent_utc DESC LIMIT 1) prev ON true
ON CONFLICT (event_id) DO UPDATE SET
    alert_level = EXCLUDED.alert_level, color_code = EXCLUDED.color_code,
    observatory = EXCLUDED.observatory, sent_utc = EXCLUDED.sent_utc,
    alert_level_ord = EXCLUDED.alert_level_ord, coords_missing = EXCLUDED.coords_missing"""],

    "volcano_updates": ["""
WITH up AS (
    INSERT INTO volcano_update (record_id, content_hash, source_id, record_type, observatory, volcano_name,
        volcano_number, alert_level, color_code, as_of_time, issued_raw, notice_number, notice_url,
        text_clean, tokens, token_count, kw_eruption, kw_ash, kw_lava, kw_seismicity, kw_deformation,
        kw_gas, kw_plume, bronze_file, bronze_sha256, silver_processed_utc)
    SELECT s.record_id, s.content_hash, s.source_id, s.record_type, s.observatory, s.volcano_name,
        coalesce(byid.volcano_number, byname.volcano_number), s.alert_level, s.color_code, s.as_of_time,
        s.issued_raw, s.notice_number, s.notice_url, s.text_clean, coalesce(s.tokens, '{}'),
        coalesce(s.token_count, 0), s.kw_eruption, s.kw_ash, s.kw_lava, s.kw_seismicity, s.kw_deformation,
        s.kw_gas, s.kw_plume, s.bronze_file, s.bronze_sha256, s.processed_utc
    FROM stage s
    LEFT JOIN volcano byid ON byid.volcano_number = s.vnum
    LEFT JOIN LATERAL (SELECT min(v.volcano_number) AS volcano_number FROM volcano v
                       WHERE lower(v.name) = lower(trim(s.volcano_name)) HAVING count(*) = 1) byname ON true
    ON CONFLICT (record_id) DO UPDATE SET
        content_hash = EXCLUDED.content_hash, volcano_name = EXCLUDED.volcano_name,
        volcano_number = EXCLUDED.volcano_number, alert_level = EXCLUDED.alert_level,
        color_code = EXCLUDED.color_code, as_of_time = EXCLUDED.as_of_time, issued_raw = EXCLUDED.issued_raw,
        notice_number = EXCLUDED.notice_number, notice_url = EXCLUDED.notice_url,
        text_clean = EXCLUDED.text_clean, tokens = EXCLUDED.tokens, token_count = EXCLUDED.token_count,
        kw_eruption = EXCLUDED.kw_eruption, kw_ash = EXCLUDED.kw_ash, kw_lava = EXCLUDED.kw_lava,
        kw_seismicity = EXCLUDED.kw_seismicity, kw_deformation = EXCLUDED.kw_deformation,
        kw_gas = EXCLUDED.kw_gas, kw_plume = EXCLUDED.kw_plume, bronze_file = EXCLUDED.bronze_file,
        bronze_sha256 = EXCLUDED.bronze_sha256, silver_processed_utc = EXCLUDED.silver_processed_utc,
        updated_at = now()
    WHERE volcano_update.content_hash IS DISTINCT FROM EXCLUDED.content_hash
    RETURNING record_id, source_id, (xmax = 0) AS is_new)
INSERT INTO changed SELECT * FROM up"""],

    "quarantine": ["""
INSERT INTO rejected_record (run_id, source_id, reason, payload, bronze_file, silver_processed_utc)
SELECT s.run_id, src.source_id, s.reason, s.payload::jsonb, s.bronze_file, s.processed_utc
FROM stage s LEFT JOIN source src ON src.source_id = s.source_id"""],
}


# --------------------------------------------------------------------------- database side

def ensure_schema(cur) -> None:
    cur.execute("SELECT to_regclass('public.silver_load_file'), to_regclass('public.volcano_update')")
    if None in cur.fetchone():
        raise SchemaMissing("project database is missing loader tables: apply sql/002_silver_indicators.sql "
                            "and sql/003_loader_state.sql (see docs/database_handoff.md)")


def loaded_files(cur) -> set[str]:
    cur.execute("SELECT file_name FROM silver_load_file")
    return {r[0] for r in cur.fetchall()}


def _known_sources(cur) -> set[str]:
    cur.execute("SELECT source_id FROM source")
    return {r[0] for r in cur.fetchall()}


def _start_runs(cur, mode: str, sources: list[str | None], file_name: str) -> dict:
    runs = {}
    for source in sources:
        cur.execute("INSERT INTO ingestion_run (mode, source_id, status, raw_file) VALUES (%s, %s, 'running', %s) "
                    "RETURNING run_id", (mode, source, file_name))
        runs[source] = cur.fetchone()[0]
    return runs


def _counts(cur, topic: str, run_ids: list[int]) -> dict[str | None, dict]:
    counts: dict = {}
    if topic != "quarantine":
        cur.execute("SELECT source_id, count(*) FILTER (WHERE is_new), count(*) FILTER (WHERE NOT is_new) "
                    "FROM changed GROUP BY source_id")
        for source, new, updated in cur.fetchall():
            counts.setdefault(source, {})["new"], counts[source]["updated"] = new, updated
    cur.execute("SELECT r.source_id, count(*) FROM rejected_record r "
                "WHERE r.run_id = ANY(%s) GROUP BY r.source_id", (run_ids,))
    for source, rejected in cur.fetchall():
        counts.setdefault(source, {})["rejected"] = rejected
    return counts


def _advance_watermarks(cur, marks: dict[str, tuple[str, str]], runs: dict) -> None:
    for source, (kind, value) in marks.items():
        new_value = ("GREATEST(ingestion_state.watermark, EXCLUDED.watermark)" if kind == "time"
                     else "EXCLUDED.watermark")
        cur.execute(f"""
INSERT INTO ingestion_state (source_id, watermark, last_success, last_run_id) VALUES (%s, %s, now(), %s)
ON CONFLICT (source_id) DO UPDATE SET watermark = {new_value}, last_success = now(),
    last_run_id = EXCLUDED.last_run_id""", (source, value, runs[source]))


def load_file(conn, topic: str, rel_name: str, path: Path, mode: str, cfg: dict) -> dict:
    """Load one Silver file in one transaction. Returns a summary dict."""
    df = pd.read_parquet(path)
    prepared = PREPARE[topic](df)
    stage = prepared.stage.copy()
    with conn.transaction(), conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(%s)", (LOCK_KEY,))
        cur.execute("SELECT 1 FROM silver_load_file WHERE file_name = %s", (rel_name,))
        if cur.fetchone():
            return {"file": rel_name, "status": "already_loaded"}

        known = _known_sources(cur)
        stage["source_id"] = [s if s in known else None for s in stage["source_id"]]
        reject_sources = {r["source_id"] if r["source_id"] in known else None for r in prepared.rejects}
        sources = sorted(set(stage["source_id"]) | reject_sources, key=lambda s: (s is None, s or ""))
        runs = _start_runs(cur, mode, sources, rel_name)
        stage["run_id"] = [runs[s] for s in stage["source_id"]]

        rows_in = {s: int((stage["source_id"] == s).sum()) if s is not None else int(stage["source_id"].isna().sum())
                   for s in sources}
        for r in prepared.rejects:
            s = r["source_id"] if r["source_id"] in known else None
            rows_in[s] = rows_in.get(s, 0) + 1

        if prepared.rejects:
            rej = pd.DataFrame(prepared.rejects)
            rej["source_id"] = [s if s in known else None for s in rej["source_id"]]
            rej["run_id"] = [runs[s] for s in rej["source_id"]]
            copy_stage(cur, "prep_rejects", REJECT_COLUMNS, rej)
            cur.execute("INSERT INTO rejected_record (run_id, source_id, reason, payload, bronze_file, "
                        "silver_processed_utc) SELECT run_id, source_id, reason, payload::jsonb, bronze_file, "
                        "processed_utc FROM prep_rejects")

        copy_stage(cur, "stage", STAGE_COLUMNS[topic], stage)
        cur.execute("CREATE TEMP TABLE changed (key text, source_id text, is_new bool) ON COMMIT DROP")
        if topic == "alert_notices":
            _stage_extra_volcanoes(cur, cfg)
        params = {"gvp_version": _source_cfg(cfg, "gvp_volcanoes").get("database_version")}
        for statement in SQL[topic]:
            cur.execute(statement, params if "%(" in statement else None)

        counts = _counts(cur, topic, list(runs.values()))
        totals = {"new": 0, "updated": 0, "rejected": 0}
        for source, run_id in runs.items():
            c = counts.get(source, {})
            for k in totals:
                totals[k] += c.get(k, 0)
            cur.execute("UPDATE ingestion_run SET finished = now(), status = 'success', rows_in = %s, rows_new = %s, "
                        "rows_updated = %s, rows_rejected = %s WHERE run_id = %s",
                        (rows_in.get(source, 0), c.get("new", 0), c.get("updated", 0), c.get("rejected", 0), run_id))
        marks = {s: m for s, m in watermark_candidates(topic, stage, cfg).items() if s in runs}
        _advance_watermarks(cur, marks, runs)
        cur.execute("INSERT INTO silver_load_file (file_name, topic, rows_in, rows_new, rows_updated, rows_rejected, "
                    "run_ids) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (rel_name, topic, sum(rows_in.values()), totals["new"], totals["updated"], totals["rejected"],
                     list(runs.values())))
    return {"file": rel_name, "status": "loaded", "rows_in": sum(rows_in.values()), **totals}


def _stage_extra_volcanoes(cur, cfg: dict) -> None:
    rows = []
    for v in extra_volcanoes(cfg):
        digest = hashlib.sha256(json.dumps(v, sort_keys=True).encode()).hexdigest()
        rows.append({"vnum": int(v["vnum"]), "name": v["name"], "latitude": float(v["latitude"]),
                     "longitude": float(v["longitude"]), "observatory": v.get("observatory"), "content_hash": digest})
    copy_stage(cur, "extra_volcano", [("vnum", "int8"), ("name", "text"), ("latitude", "float8"),
                                      ("longitude", "float8"), ("observatory", "text"), ("content_hash", "text")],
               pd.DataFrame(rows, columns=["vnum", "name", "latitude", "longitude", "observatory", "content_hash"]))


def _record_failure(conn, mode: str, rel_name: str, topic: str, error: Exception) -> None:
    source = DEFAULT_SOURCE.get(topic)
    with conn.transaction(), conn.cursor() as cur:
        cur.execute("INSERT INTO ingestion_run (mode, source_id, status, raw_file, finished, error_message) "
                    "VALUES (%s, (SELECT source_id FROM source WHERE source_id = %s), 'failed', %s, now(), %s)",
                    (mode, source, rel_name, f"{type(error).__name__}: {error}"[:2000]))


def load_pending(silver_dir: Path, cfg: dict, mode: str = "incremental", conn=None) -> dict:
    """Load every Silver file not yet in the database. Returns a summary for logs and the CLI."""
    from .db import connect

    own = conn is None
    conn = conn or connect()
    summary = {"loaded": [], "failed": None, "pending": 0}
    try:
        with conn.cursor() as cur:
            ensure_schema(cur)
            done = loaded_files(cur)
        conn.commit()
        todo = [(t, rel, p) for t, rel, p in silver_files(silver_dir) if rel not in done]
        summary["pending"] = len(todo)
        for topic, rel, path in todo:
            try:
                result = load_file(conn, topic, rel, path, mode, cfg)
            except Exception as exc:
                log.exception("loading %s failed", rel)
                _record_failure(conn, mode, rel, topic, exc)
                summary["failed"] = {"file": rel, "error": f"{type(exc).__name__}: {exc}"}
                break
            log.info(json.dumps({"event": "db_load_file", **result}))
            summary["loaded"].append(result)
    finally:
        if own:
            conn.close()
    log.info(json.dumps({"event": "db_load", "files": len(summary["loaded"]), "failed": summary["failed"]}))
    return summary
