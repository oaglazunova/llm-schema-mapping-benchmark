from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from lsmbench.benchmark.task_loader import load_gold, load_task, load_task_set, load_tasks_from_task_set
from lsmbench.validators import (
    validate_downstream,
    validate_execution,
    validate_plan,
    validate_references,
    validate_task,
)


REPO_ROOT = Path(__file__).resolve().parents[3]


@dataclass
class TaskReportRow:
    task_id: str
    split: str
    difficulty: str
    source_family: str
    target_entity: str
    task_valid: bool
    plan_valid: bool
    references_valid: bool
    execution_valid: bool
    downstream_valid: bool
    overall_valid: bool
    task_error_count: int
    plan_error_count: int
    reference_error_count: int
    execution_error_count: int
    downstream_error_count: int


def _bool_to_int(x: bool) -> int:
    return 1 if x else 0


def _safe_errors(report: dict[str, Any] | None) -> list[str]:
    if not report:
        return []
    errs = report.get("errors", [])
    return errs if isinstance(errs, list) else [str(errs)]


def _task_paths_from_task_set(task_set_ref: str | Path) -> list[Path]:
    task_set = load_task_set(task_set_ref)
    task_set_path = Path(task_set["__task_set_path__"])
    base_dir = task_set_path.parent

    paths: list[Path] = []

    for task_ref in task_set["tasks"]:
        if isinstance(task_ref, str):
            p = Path(task_ref)
            if p.is_absolute():
                paths.append(p)
            elif p.parts and p.parts[0] in {"benchmark", "schemas", "docs", "src", "tests"}:
                paths.append(REPO_ROOT / p)
            else:
                paths.append((base_dir / p).resolve())
            continue

        if isinstance(task_ref, dict):
            for key in ("task_path", "path", "ref", "task_ref"):
                if key in task_ref:
                    p = Path(task_ref[key])
                    if p.is_absolute():
                        paths.append(p)
                    elif p.parts and p.parts[0] in {"benchmark", "schemas", "docs", "src", "tests"}:
                        paths.append(REPO_ROOT / p)
                    else:
                        paths.append((base_dir / p).resolve())
                    break
            else:
                task_id = task_ref.get("task_id")
                split = task_ref.get("split")

                if not split and task_id:
                    if task_id.startswith("GB_"):
                        split = "gamebus"
                    elif task_id.startswith("PUB_"):
                        split = "public"
                    elif task_id.startswith("SYN_"):
                        split = "synthetic/native"

                if task_id and split:
                    candidate = REPO_ROOT / "benchmark" / "tasks" / split / f"{task_id}_task.json"
                    if candidate.exists():
                        paths.append(candidate)
                        continue

                    if split == "synthetic":
                        candidate = REPO_ROOT / "benchmark" / "tasks" / "synthetic" / "native" / f"{task_id}_task.json"
                        if candidate.exists():
                            paths.append(candidate)
                            continue

                raise ValueError(f"Could not resolve task path from task-set entry: {task_ref!r}")
            continue

        raise TypeError(f"Unsupported task ref type: {type(task_ref)!r}")

    return paths


def evaluate_task_set(task_set: str | Path) -> dict[str, Any]:
    task_paths = _task_paths_from_task_set(task_set)

    rows: list[TaskReportRow] = []
    detailed: list[dict[str, Any]] = []

    for task_path in task_paths:
        task = load_task(task_path)
        plan = load_gold(task, "plan")

        task_report = validate_task(task)
        plan_report = validate_plan(plan)
        ref_report = validate_references(task, plan)
        exec_report = validate_execution(task, plan)

        if task.get("downstream_checks"):
            try:
                downstream_report = validate_downstream(task, plan)
            except Exception as e:
                downstream_report = {
                    "valid": False,
                    "errors": [f"Downstream validation crashed: {e}"],
                }
        else:
            downstream_report = {"valid": True, "errors": []}

        row = TaskReportRow(
            task_id=task["task_id"],
            split=task["split"],
            difficulty=task["difficulty"],
            source_family=task["source_family"],
            target_entity=task["target_entity"],
            task_valid=task_report["valid"],
            plan_valid=plan_report["valid"],
            references_valid=ref_report["valid"],
            execution_valid=exec_report["valid"],
            downstream_valid=downstream_report["valid"],
            overall_valid=all(
                [
                    task_report["valid"],
                    plan_report["valid"],
                    ref_report["valid"],
                    exec_report["valid"],
                ]
            ),
            task_error_count=len(_safe_errors(task_report)),
            plan_error_count=len(_safe_errors(plan_report)),
            reference_error_count=len(_safe_errors(ref_report)),
            execution_error_count=len(_safe_errors(exec_report)),
            downstream_error_count=len(_safe_errors(downstream_report)),
        )
        rows.append(row)

        detailed.append(
            {
                "task_id": task["task_id"],
                "task_path": str(task_path),
                "split": task["split"],
                "difficulty": task["difficulty"],
                "source_family": task["source_family"],
                "target_entity": task["target_entity"],
                "task_report": task_report,
                "plan_report": plan_report,
                "reference_report": ref_report,
                "execution_report": exec_report,
                "downstream_report": downstream_report,
            }
        )

    summary = {
        "task_count": len(rows),
        "task_valid_rate": sum(_bool_to_int(r.task_valid) for r in rows) / len(rows) if rows else 0.0,
        "plan_valid_rate": sum(_bool_to_int(r.plan_valid) for r in rows) / len(rows) if rows else 0.0,
        "references_valid_rate": sum(_bool_to_int(r.references_valid) for r in rows) / len(rows) if rows else 0.0,
        "execution_valid_rate": sum(_bool_to_int(r.execution_valid) for r in rows) / len(rows) if rows else 0.0,
        "downstream_valid_rate": sum(_bool_to_int(r.downstream_valid) for r in rows) / len(rows) if rows else 0.0,
        "overall_valid_rate": sum(_bool_to_int(r.overall_valid) for r in rows) / len(rows) if rows else 0.0,
        "by_split": {},
        "by_difficulty": {},
    }

    for split in sorted({r.split for r in rows}):
        sub = [r for r in rows if r.split == split]
        summary["by_split"][split] = {
            "task_count": len(sub),
            "overall_valid_rate": sum(_bool_to_int(r.overall_valid) for r in sub) / len(sub) if sub else 0.0,
            "execution_valid_rate": sum(_bool_to_int(r.execution_valid) for r in sub) / len(sub) if sub else 0.0,
        }

    for diff in sorted({r.difficulty for r in rows}):
        sub = [r for r in rows if r.difficulty == diff]
        summary["by_difficulty"][diff] = {
            "task_count": len(sub),
            "overall_valid_rate": sum(_bool_to_int(r.overall_valid) for r in sub) / len(sub) if sub else 0.0,
            "execution_valid_rate": sum(_bool_to_int(r.execution_valid) for r in sub) / len(sub) if sub else 0.0,
        }

    return {
        "summary": summary,
        "rows": [asdict(r) for r in rows],
        "detailed": detailed,
    }


def write_report_bundle(report: dict[str, Any], out_prefix: Path) -> tuple[Path, Path]:
    out_prefix.parent.mkdir(parents=True, exist_ok=True)

    json_path = out_prefix.with_suffix(".json")
    csv_path = out_prefix.with_suffix(".csv")

    json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    rows = report["rows"]
    if rows:
        fieldnames = list(rows[0].keys())
        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
    else:
        csv_path.write_text("", encoding="utf-8")

    return json_path, csv_path


def render_console_summary(report: dict[str, Any]) -> str:
    s = report["summary"]
    lines = [
        f"Tasks: {s['task_count']}",
        f"Task valid rate:       {s['task_valid_rate']:.3f}",
        f"Plan valid rate:       {s['plan_valid_rate']:.3f}",
        f"Reference valid rate:  {s['references_valid_rate']:.3f}",
        f"Execution valid rate:  {s['execution_valid_rate']:.3f}",
        f"Downstream valid rate: {s['downstream_valid_rate']:.3f}",
        f"Overall valid rate:    {s['overall_valid_rate']:.3f}",
        "",
        "By split:",
    ]

    for split, stats in s["by_split"].items():
        lines.append(
            f"  - {split}: n={stats['task_count']} overall={stats['overall_valid_rate']:.3f} execution={stats['execution_valid_rate']:.3f}"
        )

    lines.append("")
    lines.append("By difficulty:")
    for diff, stats in s["by_difficulty"].items():
        lines.append(
            f"  - {diff}: n={stats['task_count']} overall={stats['overall_valid_rate']:.3f} execution={stats['execution_valid_rate']:.3f}"
        )

    return "\n".join(lines)