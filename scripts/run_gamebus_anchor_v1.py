from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import sys
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from lsmbench.validators.task_validator import validate_task_bundle
from lsmbench.execution.engine import execute_plan_from_files
from lsmbench.evaluation.expected_comparator import compare_outputs
from lsmbench.evaluation.invariant_checker import check_invariants_from_file


def _load_json(path: Path) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        with open(path, "w", encoding="utf-8", newline="") as f:
            f.write("")
        return

    fieldnames = list(rows[0].keys())
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _is_placeholder_expected(expected_path: Path) -> bool:
    if not expected_path.exists():
        return True
    try:
        payload = _load_json(expected_path)
    except Exception:
        return True
    return isinstance(payload, dict) and payload.get("status") == "placeholder"


def run_one_task(task_id: str, evaluation_mode: str) -> dict[str, Any]:
    task_path = PROJECT_ROOT / "benchmark" / "tasks" / "gamebus" / f"{task_id}_task.json"
    plan_path = PROJECT_ROOT / "benchmark" / "gold" / "plans" / f"{task_id}_plan.json"
    fixture_input_path = PROJECT_ROOT / "benchmark" / "fixtures" / "gamebus" / f"{task_id}_input.sample.json"
    expected_path = PROJECT_ROOT / "benchmark" / "fixtures" / "gamebus" / f"{task_id}_expected.sample.json"

    task_schema = PROJECT_ROOT / "schemas" / "benchmark_task_v1.schema.json"
    plan_schema = PROJECT_ROOT / "schemas" / "mapping_plan_v1.schema.json"

    reports_dir = PROJECT_ROOT / "runs" / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    # 1) schema/reference validation
    validation_report = validate_task_bundle(
        task_path=task_path,
        plan_path=plan_path,
        task_schema_path=task_schema,
        plan_schema_path=plan_schema,
    )

    validation_ok = validation_report.ok
    validation_errors = len([i for i in validation_report.issues if i.level == "error"])
    validation_warnings = len([i for i in validation_report.issues if i.level == "warning"])

    # 2) execute gold plan
    execution_result = execute_plan_from_files(
        task_path=task_path,
        plan_path=plan_path,
        fixture_input_path=fixture_input_path,
    )

    execution_ok = execution_result.ok
    execution_errors = len([i for i in execution_result.issues if i.level == "error"])

    executed_outputs_path = reports_dir / f"{task_id}_executed_outputs.json"
    execution_report_path = reports_dir / f"{task_id}_execution_report.json"

    _write_json(executed_outputs_path, execution_result.outputs)
    _write_json(
        execution_report_path,
        {
            "ok": execution_result.ok,
            "issues": [
                {
                    "level": i.level,
                    "code": i.code,
                    "message": i.message,
                    "record_index": i.record_index,
                }
                for i in execution_result.issues
            ],
        },
    )

    comparison_ok = None
    invariant_ok = None
    final_status = "fail"
    evaluation_detail = ""

    if not validation_ok:
        final_status = "fail"
        evaluation_detail = "validation_failed"
    elif not execution_ok:
        final_status = "fail"
        evaluation_detail = "execution_failed"
    else:
        if evaluation_mode == "exact":
            if _is_placeholder_expected(expected_path):
                comparison_ok = None
                final_status = "pending"
                evaluation_detail = "expected_output_placeholder"
            else:
                comparison_report = compare_outputs(expected_path=expected_path, actual_path=executed_outputs_path)
                comparison_ok = comparison_report.ok
                final_status = "pass" if comparison_report.ok else "fail"
                evaluation_detail = "exact"
                _write_json(
                    reports_dir / f"{task_id}_expected_comparison_report.json",
                    {
                        "ok": comparison_report.ok,
                        "issues": [
                            {
                                "level": i.level,
                                "code": i.code,
                                "message": i.message,
                            }
                            for i in comparison_report.issues
                        ],
                    },
                )
        elif evaluation_mode == "invariants":
            invariant_report = check_invariants_from_file(task_id, executed_outputs_path)
            invariant_ok = invariant_report.ok
            final_status = "pass" if invariant_report.ok else "fail"
            evaluation_detail = "invariants"
            _write_json(
                reports_dir / f"{task_id}_invariant_report.json",
                {
                    "ok": invariant_report.ok,
                    "issues": [
                        {
                            "level": i.level,
                            "code": i.code,
                            "message": i.message,
                            "record_index": i.record_index,
                        }
                        for i in invariant_report.issues
                    ],
                },
            )
        else:
            final_status = "fail"
            evaluation_detail = f"unknown_evaluation_mode:{evaluation_mode}"

    return {
        "task_id": task_id,
        "evaluation_mode": evaluation_mode,
        "validation_ok": validation_ok,
        "validation_errors": validation_errors,
        "validation_warnings": validation_warnings,
        "execution_ok": execution_ok,
        "execution_errors": execution_errors,
        "comparison_ok": comparison_ok,
        "invariant_ok": invariant_ok,
        "final_status": final_status,
        "final_ok": final_status == "pass",
        "evaluation_detail": evaluation_detail,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the GameBus anchor benchmark task set.")
    parser.add_argument(
        "--task-set",
        type=str,
        default="benchmark/task_sets/gamebus_anchor_v1.json",
        help="Path to task-set manifest",
    )
    args = parser.parse_args()

    task_set_path = PROJECT_ROOT / args.task_set
    manifest = _load_json(task_set_path)

    rows: list[dict[str, Any]] = []
    for item in manifest["tasks"]:
        row = run_one_task(
            task_id=item["task_id"],
            evaluation_mode=item["evaluation_mode"],
        )
        rows.append(row)

        print("=" * 80)
        print(f"TASK: {row['task_id']}")
        print(f"VALIDATION_OK: {row['validation_ok']}")
        print(f"EXECUTION_OK: {row['execution_ok']}")
        print(f"EVALUATION_MODE: {row['evaluation_mode']}")
        print(f"FINAL_STATUS: {row['final_status']}")
        print(f"EVALUATION_DETAIL: {row['evaluation_detail']}")

    summary = {
        "task_set": manifest["name"],
        "task_count": len(rows),
        "passed_count": sum(1 for r in rows if r["final_status"] == "pass"),
        "pending_count": sum(1 for r in rows if r["final_status"] == "pending"),
        "failed_count": sum(1 for r in rows if r["final_status"] == "fail"),
        "rows": rows,
    }

    reports_dir = PROJECT_ROOT / "runs" / "reports"
    _write_json(reports_dir / "gamebus_anchor_v1_summary.json", summary)
    _write_csv(reports_dir / "gamebus_anchor_v1_summary.csv", rows)

    print("=" * 80)
    print(f"TASK SET: {manifest['name']}")
    print(f"PASSED:  {summary['passed_count']} / {summary['task_count']}")
    print(f"PENDING: {summary['pending_count']} / {summary['task_count']}")
    print(f"FAILED:  {summary['failed_count']} / {summary['task_count']}")
    print(f"JSON SUMMARY: {reports_dir / 'gamebus_anchor_v1_summary.json'}")
    print(f"CSV SUMMARY:  {reports_dir / 'gamebus_anchor_v1_summary.csv'}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())