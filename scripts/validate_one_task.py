from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Make src/ importable when running the script from repo root
REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from lsmbench.validators.pipeline import (
    load_json,
    load_task_bundle,
    validate_task_execution,
)


def pretty(obj) -> str:
    return json.dumps(obj, indent=2, ensure_ascii=False, sort_keys=False)


def resolve_task_path(task_ref: str) -> Path:
    """
    Accept either:
      - GB_001
      - GB_001_task.json
      - benchmark/tasks/gamebus/GB_001_task.json
    """
    candidate = Path(task_ref)

    if candidate.exists():
        return candidate.resolve()

    if task_ref.endswith("_task.json"):
        p = REPO_ROOT / "benchmark" / "tasks" / "gamebus" / task_ref
        if p.exists():
            return p.resolve()

    p = REPO_ROOT / "benchmark" / "tasks" / "gamebus" / f"{task_ref}_task.json"
    if p.exists():
        return p.resolve()

    raise FileNotFoundError(f"Could not resolve task path from: {task_ref}")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate and inspect a single benchmark task."
    )

    parser.add_argument(
        "task",
        help="Task reference: GB_001 or GB_001_task.json or full path",
    )

    parser.add_argument(
        "--schema",
        type=Path,
        default=REPO_ROOT / "schemas" / "mapping_plan_v1.schema.json",
        help="Path to mapping plan JSON schema",
    )

    parser.add_argument(
        "--show-produced",
        action="store_true",
        help="Print produced output JSON",
    )

    parser.add_argument(
        "--show-expected",
        action="store_true",
        help="Print expected output JSON",
    )

    parser.add_argument(
        "--show-plan",
        action="store_true",
        help="Print the gold plan JSON used for validation",
    )

    parser.add_argument(
        "--show-task",
        action="store_true",
        help="Print the task JSON",
    )

    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    task_path = resolve_task_path(args.task)
    bundle = load_task_bundle(task_path)

    report = validate_task_execution(
        task=bundle["task"],
        plan=bundle["gold_plan"],
        fixture_input=bundle["fixture_input"],
        fixture_expected=bundle["fixture_expected"],
        invariants=bundle["invariants"],
        mapping_plan_schema=load_json(args.schema),
    )

    print(f"Task: {task_path.name}")
    print(f"OK:   {report.ok}")

    if report.issues:
        print("\n=== Issues ===")
        for issue in report.issues:
            where = f" ({issue.path})" if issue.path else ""
            print(f"- [{issue.stage}] {issue.level.upper()}: {issue.message}{where}")
    else:
        print("\nNo issues.")

    if args.show_task:
        print("\n=== Task ===")
        print(pretty(bundle["task"]))

    if args.show_plan:
        print("\n=== Gold plan ===")
        print(pretty(bundle["gold_plan"]))

    if args.show_produced:
        print("\n=== Produced output ===")
        print(pretty(report.produced_output))

    if args.show_expected:
        print("\n=== Expected output ===")
        print(pretty(bundle["fixture_expected"]))

    return 0 if report.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())