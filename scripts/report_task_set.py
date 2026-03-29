from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from lsmbench.evaluation.reporting import (
    evaluate_task_set,
    render_console_summary,
    write_report_bundle,
)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run validation/reporting over a benchmark task set."
    )
    parser.add_argument(
        "--task-set",
        required=True,
        help="Task-set name like 'pilot_v1' or path like benchmark/task_sets/pilot_v1.json",
    )
    parser.add_argument(
        "--out-prefix",
        default=None,
        help="Optional output prefix. Default: runs/reports/<task_set_name>_summary",
    )
    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    task_set_ref = args.task_set
    task_set_name = Path(task_set_ref).stem if task_set_ref.endswith(".json") else task_set_ref

    report = evaluate_task_set(task_set_ref)

    if args.out_prefix:
        out_prefix = Path(args.out_prefix)
    else:
        out_prefix = REPO_ROOT / "runs" / "reports" / f"{task_set_name}_summary"

    json_path, csv_path = write_report_bundle(report, out_prefix)

    print(render_console_summary(report))
    print("")
    print(f"Wrote JSON: {json_path}")
    print(f"Wrote CSV:  {csv_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())