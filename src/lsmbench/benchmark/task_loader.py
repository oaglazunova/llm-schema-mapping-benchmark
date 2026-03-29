from __future__ import annotations

import json
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[3]


def _load_json(path: Path) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _resolve_repo_path(ref: str | Path, *, base_dir: Path | None = None) -> Path:
    p = Path(ref)

    if p.is_absolute():
        return p

    # Treat repo-root-relative refs explicitly
    if p.parts and p.parts[0] in {"benchmark", "schemas", "docs", "src", "tests"}:
        return REPO_ROOT / p

    # Otherwise resolve relative to the provided base dir if present
    if base_dir is not None:
        return (base_dir / p).resolve()

    return (REPO_ROOT / p).resolve()


def load_task(task_ref: str | Path) -> dict[str, Any]:
    task_path = _resolve_repo_path(task_ref)
    task = _load_json(task_path)
    task["__task_path__"] = str(task_path)
    return task


def load_task_set(task_set_ref: str | Path) -> dict[str, Any]:
    ref = Path(task_set_ref)

    if isinstance(task_set_ref, str) and ref.suffix == "":
        task_set_path = REPO_ROOT / "benchmark" / "task_sets" / f"{task_set_ref}.json"
    else:
        task_set_path = _resolve_repo_path(task_set_ref)

    task_set = _load_json(task_set_path)
    task_set["__task_set_path__"] = str(task_set_path)
    return task_set


def load_tasks_from_task_set(task_set_ref: str | Path) -> list[dict[str, Any]]:
    task_set = load_task_set(task_set_ref)
    task_set_path = Path(task_set["__task_set_path__"])
    base_dir = task_set_path.parent

    tasks: list[dict[str, Any]] = []

    def _infer_task_path_from_dict(task_ref: dict) -> Path | None:
        # Common explicit keys
        for key in ("task_path", "path", "ref", "task_ref"):
            if key in task_ref:
                return _resolve_repo_path(task_ref[key], base_dir=base_dir)

        # Fallback: infer from task_id + split
        task_id = task_ref.get("task_id")
        split = task_ref.get("split")

        if task_id:
            if not split:
                # Infer split from task_id prefix
                if task_id.startswith("GB_"):
                    split = "gamebus"
                elif task_id.startswith("PUB_"):
                    split = "public"
                elif task_id.startswith("SYN_"):
                    split = "synthetic/native"

            if split:
                candidate = REPO_ROOT / "benchmark" / "tasks" / split / f"{task_id}_task.json"
                if candidate.exists():
                    return candidate

                # secondary fallback for synthetic split without /native
                if split == "synthetic":
                    candidate = REPO_ROOT / "benchmark" / "tasks" / "synthetic" / "native" / f"{task_id}_task.json"
                    if candidate.exists():
                        return candidate

        return None

    for task_ref in task_set["tasks"]:
        if isinstance(task_ref, (str, Path)):
            task_path = _resolve_repo_path(task_ref, base_dir=base_dir)
            tasks.append(load_task(task_path))
            continue

        if isinstance(task_ref, dict):
            inferred_path = _infer_task_path_from_dict(task_ref)
            if inferred_path is not None:
                tasks.append(load_task(inferred_path))
                continue

            # last-resort fallback: embedded full task object
            task_obj = dict(task_ref)
            task_obj.setdefault("__task_path__", str(task_set_path))
            tasks.append(task_obj)
            continue

        raise TypeError(
            f"Unsupported task reference type in task set {task_set_ref!r}: {type(task_ref)!r}"
        )

    return tasks


def load_fixture(task: dict[str, Any], which: str) -> Any:
    task_path = Path(task["__task_path__"])
    base_dir = task_path.parent

    ref = task["fixture_refs"][which]
    fixture_path = _resolve_repo_path(ref, base_dir=base_dir)
    return _load_json(fixture_path)


def load_gold(task: dict[str, Any], which: str) -> Any:
    task_path = Path(task["__task_path__"])
    base_dir = task_path.parent

    ref = task["gold_refs"][which]
    gold_path = _resolve_repo_path(ref, base_dir=base_dir)
    return _load_json(gold_path)


def load_task_bundle(task_ref: str | Path) -> dict[str, Any]:
    task = load_task(task_ref)
    return {
        "task": task,
        "fixture_input": load_fixture(task, "input"),
        "fixture_expected": load_fixture(task, "expected"),
        "gold_matches": load_gold(task, "matches"),
        "gold_plan": load_gold(task, "plan"),
        "invariants": load_gold(task, "invariants"),
    }


def load_all_tasks(tasks_root: str | Path = "benchmark/tasks") -> list[dict[str, Any]]:
    root = _resolve_repo_path(tasks_root)
    tasks: list[dict[str, Any]] = []

    for task_path in sorted(root.rglob("*_task.json")):
        tasks.append(load_task(task_path))

    return tasks