from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from lsmbench.benchmark.task_loader import load_task_set, load_task, load_gold
from lsmbench.validators import validate_plan


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate gold mapping plans.")
    parser.add_argument(
        "--task-set",
        type=Path,
        required=True,
        help="Path to a task-set manifest JSON, e.g. benchmark/task_sets/synthetic_v1.json",
    )
    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    task_set = load_task_set(args.task_set)
    failures = []

    for task_ref in task_set["tasks"]:
        task = load_task(task_ref)
        plan = load_gold(task, "plan")
        report = validate_plan(plan)
        if not report["valid"]:
            failures.append((task["task_id"], report["errors"]))

    if failures:
        print("Gold plan validation failures:")
        for task_id, errors in failures:
            print(f"- {task_id}")
            for err in errors:
                print(f"    {err}")
        return 1

    print(f"All gold plans passed for task set: {task_set['name']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())