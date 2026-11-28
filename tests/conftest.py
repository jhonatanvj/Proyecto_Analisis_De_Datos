import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
# the dashboard package lives at the repo root (dashboard/), outside src/
sys.path.insert(0, str(ROOT))
# Spark runs Python UDFs (e.g. the storytelling tone scores) in separate worker processes, which
# inherit the environment but not this sys.path: give them src/ and the interpreter running pytest
# (inside Docker PYTHONPATH is already set by docker-compose.yml)
os.environ["PYTHONPATH"] = os.pathsep.join(p for p in (str(ROOT / "src"), os.environ.get("PYTHONPATH")) if p)
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
