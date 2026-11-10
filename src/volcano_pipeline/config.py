"""Load config/sources.yaml and environment variables (configuration outside code)."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = Path(os.getenv("SOURCES_CONFIG", ROOT / "config" / "sources.yaml"))


def _load_dotenv(path: Path = ROOT / ".env") -> None:
    """Minimal .env loader (no extra dependency). Existing env vars win."""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def load_config(path: Path | str = CONFIG_PATH) -> dict[str, Any]:
    _load_dotenv()
    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def get_source(cfg: dict[str, Any], source_id: str) -> dict[str, Any]:
    for src in cfg["sources"]:
        if src["source_id"] == source_id:
            return src
    raise KeyError(f"Unknown source_id: {source_id}")


def user_agent(cfg: dict[str, Any]) -> str:
    env_name = cfg["project"].get("user_agent_env", "HTTP_USER_AGENT")
    return os.getenv(env_name, "volcano-monitoring-team6/0.1 (student project; contact in .env)")


def raw_dir() -> Path:
    return Path(os.getenv("RAW_DATA_DIR", ROOT / "data" / "raw"))
