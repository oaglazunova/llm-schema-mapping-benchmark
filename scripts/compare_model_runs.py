from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from lsmbench.evaluation.model_run_io import (
    iter_scored_task_report_paths,
    load_json,
    write_csv,
    write_json,
)


def _load_scored_summary(run_dir: str | Path) -> dict:
    path = Path(run_dir) / "scored" / "summary.json"
    if not path.exists():
        raise FileNotFoundError(
            f"Missing scored summary at {path}. Run score_model_run.py first."
        )
    return load_json(path)


def _load_task_reports(run_dir: str | Path) -> dict[str, dict]:
    reports: dict[str, dict] = {}
    for path in iter_scored_task_report_paths(run_dir):
        payload = load_json(path)
        reports[payload["task_id"]] = payload
    return reports


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare multiple scored model run directories."
    )
    parser.add_argument(
        "--run-dir",
        action="append",
        required=True,
        help="Scored run directory. Repeat for multiple runs.",
    )
    parser.add_argument(
        "--out-prefix",
        required=True,
        help="Output prefix, e.g. runs/reports/pilot_v1_compare",
    )
    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    summaries = []
    for run_dir in args.run_dir:
        summaries.append(_load_scored_summary(run_dir))

    task_set_ids = {s["summary"]["task_set_id"] for s in summaries}
    if len(task_set_ids) != 1:
        raise ValueError(f"All runs must belong to the same task_set_id, got {sorted(task_set_ids)!r}")

    summary_rows = []
    per_task_reports: list[tuple[str, dict[str, dict]]] = []

    for run_dir, payload in zip(args.run_dir, summaries, strict=False):
        s = payload["summary"]
        run_id = s["run_id"]
        model_name = s["model_name"]

        summary_rows.append(
            {
                "run_id": run_id,
                "model_name": model_name,
                "task_set_id": s["task_set_id"],
                "task_count": s["task_count"],
                "overall_valid_rate": s["overall_valid_rate"],
                "execution_valid_rate": s["execution_valid_rate"],
                "downstream_valid_rate": s["downstream_valid_rate"],
                "produced_plan_rate": s["produced_plan_rate"],
                "abstention_rate": s["abstention_rate"],
                "avg_input_tokens": s["avg_input_tokens"],
                "avg_output_tokens": s["avg_output_tokens"],
                "avg_latency_ms": s["avg_latency_ms"],
                "total_cost_usd": s["total_cost_usd"],
                "run_dir": str(run_dir),
            }
        )

        per_task_reports.append((run_id, _load_task_reports(run_dir)))

    all_task_ids = sorted(
        {
            task_id
            for _, reports in per_task_reports
            for task_id in reports.keys()
        }
    )

    per_task_rows = []
    for task_id in all_task_ids:
        row = {"task_id": task_id}
        for run_id, reports in per_task_reports:
            report = reports.get(task_id)
            if report is None:
                row[f"{run_id}__overall_valid"] = None
                row[f"{run_id}__execution_valid"] = None
                row[f"{run_id}__error_codes"] = "missing_report"
                continue

            row[f"{run_id}__overall_valid"] = report["overall_valid"]
            row[f"{run_id}__execution_valid"] = report["execution_valid"]
            row[f"{run_id}__error_codes"] = ",".join(report.get("error_codes", []))

        per_task_rows.append(row)

    out_prefix = Path(args.out_prefix)
    out_prefix.parent.mkdir(parents=True, exist_ok=True)

    json_path = write_json(
        out_prefix.with_suffix(".json"),
        {
            "task_set_id": next(iter(task_set_ids)),
            "summary_rows": summary_rows,
            "per_task_rows": per_task_rows,
        },
    )
    summary_csv_path = write_csv(out_prefix.with_name(out_prefix.name + "_summary.csv"), summary_rows)
    per_task_csv_path = write_csv(out_prefix.with_name(out_prefix.name + "_per_task.csv"), per_task_rows)

    print(f"Wrote JSON:         {json_path}")
    print(f"Wrote summary CSV:  {summary_csv_path}")
    print(f"Wrote per-task CSV: {per_task_csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())