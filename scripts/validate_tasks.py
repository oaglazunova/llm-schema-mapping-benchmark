from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable

# Make src/ importable when running the script from repo root
REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from lsmbench.validators.pipeline import validate_task_file


def iter_task_paths(tasks_root: Path) -> Iterable[Path]:
    """
    Yield all benchmark task JSON files under a directory.

    Expected examples:
      benchmark/tasks/gamebus/GB_001_task.json
      benchmark/tasks/public/PUB_001_task.json
    """
    yield from sorted(tasks_root.rglob("*_task.json"))


def load_task_set(task_set_path: Path) -> list[Path]:
    """
    Load a task-set manifest such as:
      benchmark/task_sets/gamebus_anchor_v1.json

    Expected structure:
      {
        "name": "...",
        "version": "...",
        "tasks": [
          "benchmark/tasks/gamebus/GB_001_task.json",
          ...
        ]
      }
    """
    with open(task_set_path, "r", encoding="utf-8") as f:
        obj = json.load(f)

    task_refs = obj.get("tasks", [])
    if not isinstance(task_refs, list):
        raise ValueError(f"Task set has invalid 'tasks' field: {task_set_path}")

    out: list[Path] = []
    for ref in task_refs:
        if not isinstance(ref, str):
            raise ValueError(f"Task ref is not a string: {ref!r}")
        out.append(REPO_ROOT / ref)
    return out


def print_report(task_path: Path, report, *, show_passed: bool = False) -> None:
    task_name = task_path.name

    if report.ok:
        if show_passed:
            print(f"[PASS] {task_name}")
        return

    print(f"[FAIL] {task_name}")
    for issue in report.issues:
        where = f" ({issue.path})" if issue.path else ""
        print(f"  - [{issue.stage}] {issue.level.upper()}: {issue.message}{where}")


def summarize(results: list[tuple[Path, object]]) -> int:
    total = len(results)
    failed = sum(1 for _, rep in results if not rep.ok)
    passed = total - failed

    print("\n=== Validation summary ===")
    print(f"Total tasks : {total}")
    print(f"Passed      : {passed}")
    print(f"Failed      : {failed}")

    return failed


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate benchmark tasks against gold plans, fixtures, and invariants."
    )

    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--tasks-root",
        type=Path,
        default=REPO_ROOT / "benchmark" / "tasks" / "gamebus",
        help="Directory containing *_task.json files (default: benchmark/tasks/gamebus)",
    )
    group.add_argument(
        "--task-set",
        type=Path,
        help="Path to a task-set manifest JSON, e.g. benchmark/task_sets/gamebus_anchor_v1.json",
    )

    parser.add_argument(
        "--schema",
        type=Path,
        default=REPO_ROOT / "schemas" / "mapping_plan_v1.schema.json",
        help="Path to mapping plan JSON schema",
    )

    parser.add_argument(
        "--show-passed",
        action="store_true",
        help="Print passing tasks as well",
    )

    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    if args.task_set:
        task_paths = load_task_set(args.task_set)
    else:
        task_paths = list(iter_task_paths(args.tasks_root))

    if not task_paths:
        print("No task files found.")
        return 2

    results: list[tuple[Path, object]] = []

    for task_path in task_paths:
        try:
            report = validate_task_file(
                task_path=task_path,
                mapping_plan_schema_path=args.schema,
            )
        except Exception as e:
            print(f"[CRASH] {task_path.name}: {e}")
            # Build a tiny pseudo-report shape compatible with summarize/print_report
            class CrashReport:
                ok = False
                issues = [
                    type(
                        "Issue",
                        (),
                        {
                            "stage": "RUNNER",
                            "level": "error",
                            "message": f"Unhandled exception: {e}",
                            "path": None,
                        },
                    )()
                ]

            report = CrashReport()

        results.append((task_path, report))
        print_report(task_path, report, show_passed=args.show_passed)

    failed = summarize(results)
    return 1 if failed > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())