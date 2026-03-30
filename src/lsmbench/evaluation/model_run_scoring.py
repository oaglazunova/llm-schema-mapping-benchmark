from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from lsmbench.benchmark.task_loader import load_tasks_from_task_set
from lsmbench.evaluation.model_run_io import (
    load_json,
    load_run_manifest,
    scored_summary_csv_path,
    scored_summary_json_path,
    scored_task_report_path,
    task_result_path,
    write_csv,
    write_json,
)
from lsmbench.validators import (
    validate_downstream,
    validate_execution,
    validate_plan,
    validate_references,
)
from lsmbench.validators.schema_validator import validate_instance_against_schema


@dataclass
class ModelRunTaskScore:
    task_id: str
    split: str
    difficulty: str
    source_family: str
    target_entity: str

    result_present: bool
    record_valid: bool
    status: str
    abstained: bool

    selected_candidate_present: bool
    produced_plan: bool

    plan_valid: bool
    references_valid: bool
    execution_valid: bool
    downstream_valid: bool
    overall_valid: bool

    input_tokens: int
    output_tokens: int
    total_tokens: int
    latency_ms: float
    cost_usd: float

    error_codes: list[str]


def _bool_to_int(x: bool) -> int:
    return 1 if x else 0


def _safe_errors(report: dict[str, Any] | None) -> list[str]:
    if not report:
        return []
    errs = report.get("errors", [])
    return errs if isinstance(errs, list) else [str(errs)]


def _usage_from_record(record: dict[str, Any] | None) -> dict[str, Any]:
    usage = (record or {}).get("usage", {})
    return {
        "input_tokens": int(usage.get("input_tokens", 0) or 0),
        "output_tokens": int(usage.get("output_tokens", 0) or 0),
        "total_tokens": int(usage.get("total_tokens", 0) or 0),
        "latency_ms": float(usage.get("latency_ms", 0.0) or 0.0),
        "cost_usd": float(usage.get("cost_usd", 0.0) or 0.0),
    }


def _record_task_consistency_errors(
    record: dict[str, Any],
    task: dict[str, Any],
    task_set_id: str,
) -> list[str]:
    errors: list[str] = []

    if record.get("task_id") != task["task_id"]:
        errors.append(
            f"Result task_id={record.get('task_id')!r} does not match benchmark task_id={task['task_id']!r}"
        )

    if record.get("task_set_id") != task_set_id:
        errors.append(
            f"Result task_set_id={record.get('task_set_id')!r} does not match scorer task_set_id={task_set_id!r}"
        )

    return errors


def _plan_task_consistency_errors(plan: dict[str, Any], task: dict[str, Any]) -> list[str]:
    errors: list[str] = []

    if plan.get("task_id") != task["task_id"]:
        errors.append(
            f"Plan task_id={plan.get('task_id')!r} does not match task task_id={task['task_id']!r}"
        )

    if plan.get("target_entity") != task["target_entity"]:
        errors.append(
            f"Plan target_entity={plan.get('target_entity')!r} does not match task target_entity={task['target_entity']!r}"
        )

    return errors


def _select_candidate(record: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
    candidates = record.get("candidates", [])
    if not candidates:
        return None, ["no_candidates"]

    selected_id = record.get("selected_candidate_id")

    if selected_id:
        for cand in candidates:
            if cand.get("candidate_id") == selected_id:
                return cand, []
        return None, [f"selected_candidate_id_not_found:{selected_id}"]

    if len(candidates) == 1:
        return candidates[0], []

    return None, ["multiple_candidates_but_no_selected_candidate_id"]


def _empty_score(task: dict[str, Any], *, status: str, error_codes: list[str]) -> ModelRunTaskScore:
    return ModelRunTaskScore(
        task_id=task["task_id"],
        split=task["split"],
        difficulty=task["difficulty"],
        source_family=task["source_family"],
        target_entity=task["target_entity"],
        result_present=False,
        record_valid=False,
        status=status,
        abstained=(status == "abstained"),
        selected_candidate_present=False,
        produced_plan=False,
        plan_valid=False,
        references_valid=False,
        execution_valid=False,
        downstream_valid=False,
        overall_valid=False,
        input_tokens=0,
        output_tokens=0,
        total_tokens=0,
        latency_ms=0.0,
        cost_usd=0.0,
        error_codes=error_codes,
    )


def score_one_task_result(
    task: dict[str, Any],
    task_set_id: str,
    run_dir: str | Path,
) -> dict[str, Any]:
    result_path = task_result_path(run_dir, task["task_id"])

    if not result_path.exists():
        score = _empty_score(task, status="missing_result", error_codes=["missing_result"])
        return {
            "row": asdict(score),
            "task_report": {
                "task_id": task["task_id"],
                "task_path": task["__task_path__"],
                "result_path": str(result_path),
                "record_valid": False,
                "selected_candidate_present": False,
                "produced_plan": False,
                "plan_valid": False,
                "references_valid": False,
                "execution_valid": False,
                "downstream_valid": False,
                "overall_valid": False,
                "error_codes": ["missing_result"],
            },
        }

    record = load_json(result_path)
    usage = _usage_from_record(record)

    record_schema_report = validate_instance_against_schema(record, "model_run_record_v1.schema.json")
    record_consistency_errors = _record_task_consistency_errors(record, task, task_set_id)

    record_valid = record_schema_report["valid"] and not record_consistency_errors
    status = record.get("status", "error")
    abstained = status == "abstained"

    selected_candidate = None
    selection_errors: list[str] = []

    if record_valid and not abstained:
        selected_candidate, selection_errors = _select_candidate(record)

    selected_candidate_present = selected_candidate is not None

    plan = selected_candidate.get("candidate_plan") if selected_candidate else None
    produced_plan = isinstance(plan, dict)

    plan_report = None
    plan_consistency_errors: list[str] = []
    references_report = None
    execution_report = None
    downstream_report = None

    plan_valid = False
    references_valid = False
    execution_valid = False
    downstream_valid = False

    error_codes: list[str] = []

    if not record_valid:
        error_codes.append("invalid_record")
    if record_consistency_errors:
        error_codes.append("record_task_mismatch")
    if abstained:
        error_codes.append("abstained")
    if selection_errors:
        error_codes.extend(selection_errors)
    if not produced_plan and not abstained:
        error_codes.append("no_candidate_plan")

    if produced_plan:
        plan_report = validate_plan(plan)
        plan_consistency_errors = _plan_task_consistency_errors(plan, task)
        plan_valid = plan_report["valid"] and not plan_consistency_errors

        if not plan_valid:
            error_codes.append("invalid_plan")

    if plan_valid:
        references_report = validate_references(task, plan)
        references_valid = references_report["valid"]

        if not references_valid:
            error_codes.append("invalid_references")

    if references_valid:
        execution_report = validate_execution(task, plan)
        execution_valid = execution_report["valid"]

        if not execution_valid:
            error_codes.append("invalid_execution")

    if execution_valid:
        if task.get("downstream_checks"):
            downstream_report = validate_downstream(task, plan)
        else:
            downstream_report = {"valid": True, "errors": []}
        downstream_valid = downstream_report["valid"]

        if not downstream_valid:
            error_codes.append("invalid_downstream")
    else:
        downstream_report = {"valid": False, "errors": []}

    overall_valid = all(
        [
            record_valid,
            not abstained,
            produced_plan,
            plan_valid,
            references_valid,
            execution_valid,
            downstream_valid,
        ]
    )

    score = ModelRunTaskScore(
        task_id=task["task_id"],
        split=task["split"],
        difficulty=task["difficulty"],
        source_family=task["source_family"],
        target_entity=task["target_entity"],
        result_present=True,
        record_valid=record_valid,
        status=status,
        abstained=abstained,
        selected_candidate_present=selected_candidate_present,
        produced_plan=produced_plan,
        plan_valid=plan_valid,
        references_valid=references_valid,
        execution_valid=execution_valid,
        downstream_valid=downstream_valid,
        overall_valid=overall_valid,
        input_tokens=usage["input_tokens"],
        output_tokens=usage["output_tokens"],
        total_tokens=usage["total_tokens"],
        latency_ms=usage["latency_ms"],
        cost_usd=usage["cost_usd"],
        error_codes=error_codes,
    )

    task_report = {
        "task_id": task["task_id"],
        "task_path": task["__task_path__"],
        "result_path": str(result_path),
        "record_valid": record_valid,
        "selected_candidate_present": selected_candidate_present,
        "produced_plan": produced_plan,
        "plan_valid": plan_valid,
        "references_valid": references_valid,
        "execution_valid": execution_valid,
        "downstream_valid": downstream_valid,
        "overall_valid": overall_valid,
        "error_codes": error_codes,
        "record_schema_report": record_schema_report,
        "record_consistency_errors": record_consistency_errors,
        "selection_errors": selection_errors,
        "plan_report": plan_report,
        "plan_consistency_errors": plan_consistency_errors,
        "references_report": references_report,
        "execution_report": execution_report,
        "downstream_report": downstream_report,
        "selected_candidate": selected_candidate,
    }

    return {
        "row": asdict(score),
        "task_report": task_report,
    }


def _avg(rows: list[dict[str, Any]], field: str) -> float:
    if not rows:
        return 0.0
    return sum(float(r.get(field, 0.0) or 0.0) for r in rows) / len(rows)


def _sum(rows: list[dict[str, Any]], field: str) -> float:
    return sum(float(r.get(field, 0.0) or 0.0) for r in rows)


def _rate(rows: list[dict[str, Any]], field: str) -> float:
    if not rows:
        return 0.0
    return sum(_bool_to_int(bool(r.get(field, False))) for r in rows) / len(rows)


def score_run_directory(
    run_dir: str | Path,
    task_set_ref: str | Path,
) -> dict[str, Any]:
    run_dir = Path(run_dir)
    task_set_id = Path(str(task_set_ref)).stem if str(task_set_ref).endswith(".json") else str(task_set_ref)

    manifest = load_run_manifest(run_dir)
    tasks = load_tasks_from_task_set(task_set_ref)

    rows: list[dict[str, Any]] = []
    task_reports: list[dict[str, Any]] = []

    for task in tasks:
        scored = score_one_task_result(task, task_set_id, run_dir)
        rows.append(scored["row"])
        task_reports.append(scored["task_report"])

    summary = {
        "run_dir": str(run_dir),
        "run_id": (manifest or {}).get("run_id", run_dir.name),
        "task_set_id": task_set_id,
        "model_name": ((manifest or {}).get("model") or {}).get("model_name", run_dir.parent.parent.name if len(run_dir.parts) >= 3 else "unknown"),
        "task_count": len(rows),
        "result_present_rate": _rate(rows, "result_present"),
        "record_valid_rate": _rate(rows, "record_valid"),
        "selected_candidate_present_rate": _rate(rows, "selected_candidate_present"),
        "produced_plan_rate": _rate(rows, "produced_plan"),
        "abstention_rate": _rate(rows, "abstained"),
        "plan_valid_rate": _rate(rows, "plan_valid"),
        "references_valid_rate": _rate(rows, "references_valid"),
        "execution_valid_rate": _rate(rows, "execution_valid"),
        "downstream_valid_rate": _rate(rows, "downstream_valid"),
        "overall_valid_rate": _rate(rows, "overall_valid"),
        "avg_input_tokens": _avg(rows, "input_tokens"),
        "avg_output_tokens": _avg(rows, "output_tokens"),
        "avg_total_tokens": _avg(rows, "total_tokens"),
        "avg_latency_ms": _avg(rows, "latency_ms"),
        "total_cost_usd": _sum(rows, "cost_usd"),
        "by_split": {},
        "by_difficulty": {},
    }

    for split in sorted({r["split"] for r in rows}):
        sub = [r for r in rows if r["split"] == split]
        summary["by_split"][split] = {
            "task_count": len(sub),
            "overall_valid_rate": _rate(sub, "overall_valid"),
            "execution_valid_rate": _rate(sub, "execution_valid"),
            "produced_plan_rate": _rate(sub, "produced_plan"),
        }

    for diff in sorted({r["difficulty"] for r in rows}):
        sub = [r for r in rows if r["difficulty"] == diff]
        summary["by_difficulty"][diff] = {
            "task_count": len(sub),
            "overall_valid_rate": _rate(sub, "overall_valid"),
            "execution_valid_rate": _rate(sub, "execution_valid"),
            "produced_plan_rate": _rate(sub, "produced_plan"),
        }

    return {
        "summary": summary,
        "rows": rows,
        "task_reports": task_reports,
    }


def write_scored_run(report: dict[str, Any], run_dir: str | Path) -> tuple[Path, Path]:
    run_dir = Path(run_dir)

    for task_report in report["task_reports"]:
        write_json(scored_task_report_path(run_dir, task_report["task_id"]), task_report)

    summary_json = write_json(scored_summary_json_path(run_dir), report)
    summary_csv = write_csv(scored_summary_csv_path(run_dir), report["rows"])

    return summary_json, summary_csv


def render_run_console_summary(report: dict[str, Any]) -> str:
    s = report["summary"]
    lines = [
        f"Run: {s['run_id']}",
        f"Model: {s['model_name']}",
        f"Task set: {s['task_set_id']}",
        f"Tasks: {s['task_count']}",
        f"Record valid rate:        {s['record_valid_rate']:.3f}",
        f"Produced plan rate:       {s['produced_plan_rate']:.3f}",
        f"Execution valid rate:     {s['execution_valid_rate']:.3f}",
        f"Downstream valid rate:    {s['downstream_valid_rate']:.3f}",
        f"Overall valid rate:       {s['overall_valid_rate']:.3f}",
        f"Avg latency (ms):         {s['avg_latency_ms']:.1f}",
        f"Total cost (USD):         {s['total_cost_usd']:.4f}",
    ]
    return "\n".join(lines)