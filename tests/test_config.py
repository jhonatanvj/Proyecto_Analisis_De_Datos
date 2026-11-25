from pathlib import Path

from volcano_pipeline.config import datalake_dir, get_source, load_config


def test_datalake_dir_follows_env(monkeypatch, tmp_path):
    monkeypatch.setenv("DATALAKE_ROOT", str(tmp_path))
    assert datalake_dir("bronze") == Path(tmp_path) / "datalake_bronze"


def test_scraping_source_is_configured():
    src = get_source(load_config(), "usgs_volcano_updates")
    assert src["url"] == "https://www.usgs.gov/programs/VHP/volcano-updates"
    assert src["format"] == "html"


def test_indicator_parameters():
    from datetime import date

    ind = load_config()["indicator"]
    assert abs(sum(ind["weights"].values()) - 1.0) < 1e-9
    assert date.fromisoformat(ind["history_start"]).weekday() == 0          # a Monday, like Spark weeks
    assert ind["seismic_window_days"] == 7
    assert set(ind["regions"]["observatories"]) == {"avo", "hvo", "cvo", "calvo", "yvo", "nmi"}
    # every region label a subregion maps to is also an observatory label (one list in the filter)
    assert set(ind["regions"]["subregions"].values()) <= set(ind["regions"]["observatories"].values())
