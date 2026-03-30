from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable


REPO_ROOT = Path(__file__).resolve().parents[3]


def load_json(path: str | Path) -> Any:
    path = Path(path)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: str | Path, payload: Any) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    return path


def write_csv(path: str | Path, rows: list[dict[str, Any]]) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if not rows:
        with open(path, "w", encoding="utf-8", newline="") as f:
            f.write("")
        return path

    fieldnames = list(rows[0].keys())
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    return path


def load_run_manifest(run_dir: str | Path) -> dict[str, Any] | None:
    path = Path(run_dir) / "run_manifest.json"
    if not path.exists():
        return None
    return load_json(path)


def task_result_path(run_dir: str | Path, task_id: str) -> Path:
    return Path(run_dir) / "task_results" / f"{task_id}.json"


def scored_dir(run_dir: str | Path) -> Path:
    return Path(run_dir) / "scored"


def scored_task_report_path(run_dir: str | Path, task_id: str) -> Path:
    return scored_dir(run_dir) / "task_reports" / f"{task_id}.json"


def scored_summary_json_path(run_dir: str | Path) -> Path:
    return scored_dir(run_dir) / "summary.json"


def scored_summary_csv_path(run_dir: str | Path) -> Path:
    return scored_dir(run_dir) / "summary.csv"


def iter_scored_task_report_paths(run_dir: str | Path) -> list[Path]:
    root = scored_dir(run_dir) / "task_reports"
    if not root.exists():
        return []
    return sorted(root.glob("*.json"))