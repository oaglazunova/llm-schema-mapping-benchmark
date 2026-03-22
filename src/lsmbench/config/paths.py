from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]

BENCHMARK_DIR = PROJECT_ROOT / "benchmark"
DATA_DIR = PROJECT_ROOT / "data"
RAW_PRIVATE_DIR = DATA_DIR / "raw_private"
INTERIM_PRIVATE_DIR = DATA_DIR / "interim_private"
DESCRIPTOR_PROFILES_DIR = INTERIM_PRIVATE_DIR / "descriptor_profiles"

SCHEMAS_DIR = PROJECT_ROOT / "schemas"
TASKS_DIR = BENCHMARK_DIR / "tasks"
FIXTURES_DIR = BENCHMARK_DIR / "fixtures"
GOLD_DIR = BENCHMARK_DIR / "gold"


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path