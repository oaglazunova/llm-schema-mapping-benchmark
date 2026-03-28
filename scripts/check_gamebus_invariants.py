from __future__ import annotations

import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from lsmbench.evaluation.invariant_checker import check_invariants_from_file, save_report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Check task-specific invariants on executed GameBus outputs."
    )
    parser.add_argument(
        "task_ids",
        nargs="+",
        help="One or more task ids, e.g. GB_004 GB_007",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    for task_id in args.task_ids:
        outputs_path = PROJECT_ROOT / "runs" / "reports" / f"{task_id}_executed_outputs.json"
        report_path = PROJECT_ROOT / "runs" / "reports" / f"{task_id}_invariant_report.json"

        report = check_invariants_from_file(task_id, outputs_path)
        save_report(report, report_path)

        print("=" * 80)
        print(f"TASK: {task_id}")
        print(f"OK: {report.ok}")
        print(f"REPORT: {report_path}")
        if report.issues:
            for issue in report.issues:
                print(f"[{issue.level.upper()}] {issue.code}: {issue.message} (record={issue.record_index})")
        else:
            print("All invariants passed.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())