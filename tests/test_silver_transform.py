"""Silver preprocessing per topic, on Bronze envelopes built from real Workshop 1 samples."""
import json
from pathlib import Path

import pandas as pd

from volcano_pipeline.silver.transform import TRANSFORMS, build_context, parse_us_time

BRONZE = Path(__file__).parent / "fixtures" / "bronze"


def load_env(name):
    return json.loads((BRONZE / name).read_text(encoding="utf-8"))


VOLCANOES = load_env("gvp_volcanoes_20261116_000000.json")
CTX = build_context(VOLCANOES, radius_km=30, min_magnitude=1.0)


def test_context_from_gvp_volcanoes():
    assert len(CTX["volcano_coords"]) == 4 and {311120, 312170, 332010} <= set(CTX["volcano_coords"])
    assert CTX["volcano_coords"][332010] == (19.421, -155.287)
    assert build_context(None)["volcano_coords"] == {}


def test_feed_nulls_and_rejects():
    topic, df, rej = TRANSFORMS["usgs_feed"](load_env("usgs_feed_20261116_010000.json"), CTX)
    assert topic == "earthquakes"
    assert sorted(r["reason"] for r in rej) == ["missing_coordinates", "non_tectonic_type"]
    assert df.loc[df["record_id"] == "usgs:hvtestnomag", "mag_missing"].item()
    assert df.loc[df["record_id"] == "usgs:hvtestneg", "depth_km"].item() == -1.2
    assert str(df["event_time"].dtype) == "datetime64[ns, UTC]"
    assert set(df["nearest_vnum"]) == {332010}
    assert len(df) == 6                       # the earthquake in the southern hemisphere is out of scope
    assert df.attrs["out_of_scope"] == 1


def test_feed_without_context_keeps_everything():
    _, df, _ = TRANSFORMS["usgs_feed"](load_env("usgs_feed_20261116_010000.json"), build_context(None))
    assert len(df) == 7 and df["nearest_vnum"].isna().all()


def test_hans_rejects_and_ordinal():
    env = load_env("usgs_hans_20261116_010000.json")
    topic, df, rej = TRANSFORMS["usgs_hans"](env, CTX)
    assert topic == "alert_notices"
    assert len(df) + len(rej) == len(env["payload"])
    assert {"region_level_notice", "no_notice_issued"} <= {r["reason"] for r in rej}
    order = {"NORMAL": 1, "ADVISORY": 2, "WATCH": 3, "WARNING": 4}
    assert (df["alert_level"].astype(str).map(order) == df["alert_level_ord"]).all()
    assert df["record_id"].str.match(r"^usgs_hans:\d{6}:DOI-USGS-").all()
    great_sitkin = df[df["vnum"] == 311120]
    assert not great_sitkin.empty and not great_sitkin["coords_missing"].any()
    assert df.loc[df["vnum"] != 311120, "coords_missing"].any()   # most are not in the 4-volcano context


def test_eruptions_partial_bce_and_scope():
    topic, df, _ = TRANSFORMS["gvp_eruptions"](load_env("gvp_eruptions_20261116_000000.json"), CTX)
    assert topic == "eruptions"
    assert 99999901 not in set(df["eruption_number"])             # non-US volcano dropped
    row = df.set_index("eruption_number")
    assert str(row.loc[22380, "time_precision"]) == "day"
    assert row.loc[22380, "start_time"] == pd.Timestamp("2020-12-20", tz="UTC")
    assert str(row.loc[20745, "time_precision"]) == "year"
    assert row.loc[20745, "start_time"] == pd.Timestamp("1510-01-01", tz="UTC")   # before 1677: needs us precision
    assert pd.isna(row.loc[20733, "start_time"]) and row.loc[20733, "start_year"] == -4650
    assert row.loc[19729, "vei_missing"] and pd.isna(row.loc[19729, "vei"])


def _eruption(number, **props):
    base = {"Eruption_Number": number, "Volcano_Number": 332010, "StartDateYear": 2024, "StartDateMonth": 12,
            "StartDateDay": 23}
    return {"type": "Feature", "geometry": {"type": "Point", "coordinates": [-155.287, 19.421]},
            "properties": base | props}


def test_eruption_end_and_continuing_flag():
    """GVP marks an eruption still going on with EndDateYearModifier '>' (continuing as of that date)."""
    env = {"source_id": "gvp_eruptions", "payload": {"features": [
        _eruption(1, EndDateYear=2026, EndDateMonth=3, EndDateDay=25, EndDateYearModifier=">"),
        _eruption(2, EndDateYear=2024, EndDateMonth=6, EndDateDay=3, EndDateYearModifier=None),
        _eruption(3),
    ]}}
    _, df, _ = TRANSFORMS["gvp_eruptions"](env, CTX)
    row = df.set_index("eruption_number")
    assert row.loc[1, "end_time"] == pd.Timestamp("2026-03-25", tz="UTC")
    assert str(row.loc[1, "end_precision"]) == "day" and bool(row.loc[1, "is_continuing"]) is True
    assert bool(row.loc[2, "is_continuing"]) is False and row.loc[2, "end_time"] == pd.Timestamp("2024-06-03", tz="UTC")
    assert pd.isna(row.loc[3, "end_time"]) and str(row.loc[3, "end_precision"]) == "unknown"
    assert bool(row.loc[3, "is_continuing"]) is False


def test_volcanoes_unknown_year():
    topic, df, _ = TRANSFORMS["gvp_volcanoes"](VOLCANOES, CTX)
    assert topic == "volcanoes" and len(df) == 4
    assert df["last_eruption_year"].isna().sum() == 1
    assert df.loc[df["vnum"] == 332010, "elevation_m"].item() > 0


HANS_VOLCANOES = {"source_id": "usgs_hans_volcanoes", "payload": json.loads(
    (Path(__file__).parent / "fixtures" / "hans_notices" / "us_volcanoes.json").read_text(encoding="utf-8"))}


def test_hans_volcanoes_by_vnum():
    from volcano_pipeline.silver.transform import hans_volcanoes_by_vnum

    by_vnum = hans_volcanoes_by_vnum(HANS_VOLCANOES)
    assert by_vnum[332010] == {"observatory": "hvo", "region": "Hawaii"}
    assert by_vnum[311120]["observatory"] == "avo"


def test_volcanoes_take_observatory_and_region_from_hans():
    from volcano_pipeline.silver.transform import hans_volcanoes_by_vnum

    by_vnum = hans_volcanoes_by_vnum(HANS_VOLCANOES)
    del by_vnum[244001]        # as for the 12 US volcanoes the real HANS list does not have
    _, df, _ = TRANSFORMS["gvp_volcanoes"](VOLCANOES, CTX | {"hans_by_vnum": by_vnum})
    row = df.set_index("vnum")
    assert row.loc[332010, "observatory"] == "hvo" and row.loc[332010, "usgs_region"] == "Hawaii"
    assert row.loc[332010, "subregion"] == "Hawaiian-Emperor Hotspot Volcano Group"
    # without a HANS entry only the GVP subregion is known
    assert pd.isna(row.loc[244001, "observatory"]) and pd.isna(row.loc[244001, "usgs_region"])
    assert row.loc[244001, "subregion"] == "Samoan Hotspot Volcano Group"


def test_volcano_updates_text():
    topic, df, rej = TRANSFORMS["usgs_volcano_updates"](load_env("usgs_volcano_updates_20261116_010000.json"), CTX)
    assert topic == "volcano_updates" and rej == []
    assert not df["text_clean"].str.contains("<").any()
    hvo = df[(df["observatory"] == "hvo") & (df["record_type"] == "observatory_notice")].iloc[0]
    assert "kīlauea" in hvo["text_clean"] and hvo["kw_eruption"] and hvo["kw_gas"]
    assert hvo["token_count"] == len(hvo["tokens"]) > 20
    assert df["record_id"].is_unique
    assert df["as_of_time"].notna().all()


def test_parse_us_time_zones():
    assert parse_us_time("Monday, November 16, 2026, 10:48 AM AKST") == pd.Timestamp("2026-11-16 19:48", tz="UTC")
    assert parse_us_time("2026-11-16 17:26:55 UTC") == pd.Timestamp("2026-11-16 17:26:55", tz="UTC")
    assert parse_us_time(None) is None and parse_us_time("not a date") is None


def test_fdsn_backfill_uses_the_earthquake_transform():
    env = load_env("usgs_feed_20261116_010000.json")
    env["source_id"] = "usgs_fdsn"
    topic, df, _ = TRANSFORMS["usgs_fdsn"](env, CTX)
    assert topic == "earthquakes" and len(df) == 6 and set(df["nearest_vnum"]) == {332010}


def test_nearest_volcano_is_vectorized_and_correct():
    from volcano_pipeline.normalization.geo import haversine_km
    from volcano_pipeline.silver.transform import nearest_volcanoes
    coords = {1: (19.421, -155.287), 2: (52.076, -176.13), 3: (58.27, -154.96)}
    lats, lons = [19.5, 52.0, 0.0], [-155.3, -176.0, 0.0]
    vnums, dists = nearest_volcanoes(lats, lons, coords)
    assert list(vnums) == [1, 2, 3]
    for lat, lon, v, d in zip(lats, lons, vnums, dists):
        expected = min(coords, key=lambda k: haversine_km(lat, lon, *coords[k]))
        assert v == expected and abs(d - haversine_km(lat, lon, *coords[v])) < 1e-6
