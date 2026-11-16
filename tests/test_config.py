from pathlib import Path

from volcano_pipeline.config import datalake_dir, get_source, load_config


def test_datalake_dir_follows_env(monkeypatch, tmp_path):
    monkeypatch.setenv("DATALAKE_ROOT", str(tmp_path))
    assert datalake_dir("bronze") == Path(tmp_path) / "datalake_bronze"


def test_scraping_source_is_configured():
    src = get_source(load_config(), "usgs_volcano_updates")
    assert src["url"] == "https://www.usgs.gov/programs/VHP/volcano-updates"
    assert src["format"] == "html"
