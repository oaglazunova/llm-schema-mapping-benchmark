from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from lsmbench.execution.engine import execute_plan_from_files


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Execute a GameBus gold plan on sample fixture input and save outputs."
    )
    parser.add_argument(
        "task_ids",
        nargs="+",
        help="One or more task ids, e.g. GB_001 GB_004 GB_007",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    for task_id in args.task_ids:
        task_path = PROJECT_ROOT / "benchmark" / "tasks" / "gamebus" / f"{task_id}_task.json"
        plan_path = PROJECT_ROOT / "benchmark" / "gold" / "plans" / f"{task_id}_plan.json"
        fixture_input_path = PROJECT_ROOT / "benchmark" / "fixtures" / "gamebus" / f"{task_id}_input.sample.json"

        result = execute_plan_from_files(
            task_path=task_path,
            plan_path=plan_path,
            fixture_input_path=fixture_input_path,
        )

        out_dir = PROJECT_ROOT / "runs" / "reports"
        out_dir.mkdir(parents=True, exist_ok=True)

        output_path = out_dir / f"{task_id}_executed_outputs.json"
        report_path = out_dir / f"{task_id}_execution_report.json"

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(result.outputs, f, indent=2, ensure_ascii=False)

        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "ok": result.ok,
                    "issues": [
                        {
                            "level": i.level,
                            "code": i.code,
                            "message": i.message,
                            "record_index": i.record_index,
                        }
                        for i in result.issues
                    ],
                },
                f,
                indent=2,
                ensure_ascii=False,
            )

        print("=" * 80)
        print(f"TASK: {task_id}")
        print(f"OK: {result.ok}")
        print(f"OUTPUTS: {output_path}")
        print(f"REPORT:  {report_path}")
        if result.issues:
            for issue in result.issues:
                print(f"[{issue.level.upper()}] {issue.code}: {issue.message} (record={issue.record_index})")
        else:
            print("No execution issues found.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())