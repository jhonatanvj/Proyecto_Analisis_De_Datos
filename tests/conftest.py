import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
# the dashboard package lives at the repo root (dashboard/), outside src/
sys.path.insert(0, str(ROOT))
