from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable


REPO_ROOT = Path(__file__).resolve().parents[3]


def repo_root() -> Path:
    return REPO_ROOT


def _as_path(path_or_name: str | Path) -> Path:
    """
    Convert either:
    - an absolute path
    - a repo-relative path like 'benchmark/tasks/gamebus/GB_001_task.json'
    - a bare task-set name like 'pilot_v1'
    into an absolute Path.
    """
    if isinstance(path_or_name, Path):
        path = path_or_name
    else:
        path = Path(path_or_name)

    if path.is_absolute():
        return path

    # allow bare task-set names like "pilot_v1"
    if path.suffix == "":
        candidate = REPO_ROOT / "benchmark" / "task_sets" / f"{path.name}.json"
        if candidate.exists():
            return candidate

    return REPO_ROOT / path


def load_json(path_or_name: str | Path) -> dict[str, Any]:
    path = _as_path(path_or_name)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_task(task_path: str | Path) -> dict[str, Any]:
    """
    Load a single benchmark task JSON.
    """
    task = load_json(task_path)

    required = {
        "task_id",
        "title",
        "split",
        "difficulty",
        "source_family",
        "target_entity",
        "task_text",
        "source_schema",
        "target_schema",
        "fixture_refs",
        "gold_refs",
        "tags",
    }
    missing = required - set(task.keys())
    if missing:
        raise ValueError(f"Task file {task_path} is missing required keys: {sorted(missing)}")

    return task


def load_task_set(task_set: str | Path) -> dict[str, Any]:
    """
    Load a task-set JSON.
    Accepts either:
    - 'pilot_v1'
    - 'benchmark/task_sets/pilot_v1.json'
    - an absolute path
    """
    out = load_json(task_set)

    required = {"name", "version", "tasks"}
    missing = required - set(out.keys())
    if missing:
        raise ValueError(f"Task set {task_set} is missing required keys: {sorted(missing)}")

    if not isinstance(out["tasks"], list):
        raise ValueError(f"Task set {task_set} must contain a list under 'tasks'.")

    return out


def load_tasks_from_task_set(task_set: str | Path) -> list[dict[str, Any]]:
    """
    Load all tasks referenced by a task-set JSON.
    """
    task_set_obj = load_task_set(task_set)
    return [load_task(task_ref) for task_ref in task_set_obj["tasks"]]


def iter_task_files(split: str | None = None) -> Iterable[Path]:
    """
    Iterate over benchmark task files on disk.

    If split is provided, it must be one of:
    - 'gamebus'
    - 'public'
    - 'synthetic'
    """
    tasks_root = REPO_ROOT / "benchmark" / "tasks"

    if split is None:
        yield from sorted(tasks_root.glob("*/*_task.json"))
        return

    split_dir = tasks_root / split
    if not split_dir.exists():
        raise ValueError(f"Unknown or missing split directory: {split_dir}")

    yield from sorted(split_dir.glob("*_task.json"))


def load_all_tasks(split: str | None = None) -> list[dict[str, Any]]:
    """
    Load all tasks, optionally restricted to one split.
    """
    return [load_task(path) for path in iter_task_files(split=split)]


def load_fixture(task: dict[str, Any], kind: str) -> dict[str, Any]:
    """
    Load one fixture for a task.

    kind must be one of:
    - 'input'
    - 'expected'
    """
    if kind not in {"input", "expected"}:
        raise ValueError(f"Unknown fixture kind: {kind}")

    fixture_refs = task["fixture_refs"]
    if kind not in fixture_refs:
        raise ValueError(f"Task {task['task_id']} has no fixture ref for '{kind}'")

    return load_json(fixture_refs[kind])


def load_gold(task: dict[str, Any], kind: str) -> Any:
    """
    Load one gold artifact for a task.

    kind must be one of:
    - 'matches'
    - 'plan'
    - 'invariants'
    """
    if kind not in {"matches", "plan", "invariants"}:
        raise ValueError(f"Unknown gold artifact kind: {kind}")

    gold_refs = task["gold_refs"]
    if kind not in gold_refs:
        raise ValueError(f"Task {task['task_id']} has no gold ref for '{kind}'")

    return load_json(gold_refs[kind])