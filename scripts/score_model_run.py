from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from lsmbench.evaluation.model_run_scoring import (
    render_run_console_summary,
    score_run_directory,
    write_scored_run,
)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Score one model run directory against a benchmark task set."
    )
    parser.add_argument(
        "--run-dir",
        required=True,
        help="Run directory, e.g. runs/gpt-4.1-mini/pilot_v1/run_001",
    )
    parser.add_argument(
        "--task-set",
        required=True,
        help="Task-set name like 'pilot_v1' or path like benchmark/task_sets/pilot_v1.json",
    )
    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    report = score_run_directory(args.run_dir, args.task_set)
    json_path, csv_path = write_scored_run(report, args.run_dir)

    print(render_run_console_summary(report))
    print("")
    print(f"Wrote JSON: {json_path}")
    print(f"Wrote CSV:  {csv_path}")
    print(f"Wrote task reports under: {Path(args.run_dir) / 'scored' / 'task_reports'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())