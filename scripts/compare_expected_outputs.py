from __future__ import annotations

import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from lsmbench.evaluation.expected_comparator import compare_outputs, save_report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare executed outputs against expected.sample.json files."
    )
    parser.add_argument(
        "task_ids",
        nargs="+",
        help="One or more task ids, e.g. GB_001 GB_004",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    for task_id in args.task_ids:
        expected_path = PROJECT_ROOT / "benchmark" / "fixtures" / "gamebus" / f"{task_id}_expected.sample.json"
        actual_path = PROJECT_ROOT / "runs" / "reports" / f"{task_id}_executed_outputs.json"
        report_path = PROJECT_ROOT / "runs" / "reports" / f"{task_id}_expected_comparison_report.json"

        report = compare_outputs(expected_path=expected_path, actual_path=actual_path)
        save_report(report, report_path)

        print("=" * 80)
        print(f"TASK: {task_id}")
        print(f"OK: {report.ok}")
        print(f"REPORT: {report_path}")
        if report.issues:
            for issue in report.issues:
                print(f"[{issue.level.upper()}] {issue.code}: {issue.message}")
        else:
            print("Expected outputs match exactly.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())