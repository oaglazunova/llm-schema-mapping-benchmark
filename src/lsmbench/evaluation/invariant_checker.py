from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any


@dataclass
class InvariantIssue:
    level: str
    code: str
    message: str
    record_index: int | None = None


@dataclass
class InvariantReport:
    ok: bool
    issues: list[InvariantIssue]


def _load_json(path: str | Path) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _check_gb004(outputs: list[dict[str, Any]]) -> list[InvariantIssue]:
    issues: list[InvariantIssue] = []

    for idx, rec in enumerate(outputs):
        consent_items = rec.get("consent_items")
        accepted_count = rec.get("accepted_count")
        item_count = rec.get("item_count")
        all_required_accepted = rec.get("all_required_accepted")

        if not isinstance(consent_items, list):
            issues.append(
                InvariantIssue(
                    level="error",
                    code="consent_items_not_list",
                    message="consent_items must be a list",
                    record_index=idx,
                )
            )
            continue

        true_count = sum(1 for item in consent_items if isinstance(item, dict) and item.get("accepted") is True)
        total_count = len(consent_items)
        all_true = all(isinstance(item, dict) and item.get("accepted") is True for item in consent_items)

        if accepted_count != true_count:
            issues.append(
                InvariantIssue(
                    level="error",
                    code="accepted_count_mismatch",
                    message=f"accepted_count={accepted_count!r}, expected {true_count}",
                    record_index=idx,
                )
            )

        if item_count != total_count:
            issues.append(
                InvariantIssue(
                    level="error",
                    code="item_count_mismatch",
                    message=f"item_count={item_count!r}, expected {total_count}",
                    record_index=idx,
                )
            )

        if all_required_accepted != all_true:
            issues.append(
                InvariantIssue(
                    level="error",
                    code="all_required_accepted_mismatch",
                    message=f"all_required_accepted={all_required_accepted!r}, expected {all_true}",
                    record_index=idx,
                )
            )

    return issues


def _check_gb007(outputs: list[dict[str, Any]]) -> list[InvariantIssue]:
    issues: list[InvariantIssue] = []

    for idx, rec in enumerate(outputs):
        activity_type_obj = rec.get("activity_type_obj")

        if activity_type_obj is None:
            # Allowed for null source values
            continue

        if not isinstance(activity_type_obj, dict):
            issues.append(
                InvariantIssue(
                    level="error",
                    code="activity_type_obj_not_object",
                    message=f"activity_type_obj must be dict or null, got {type(activity_type_obj).__name__}",
                    record_index=idx,
                )
            )
            continue

        # Optional semantic checks for known keys in parsed tizen activity objects
        for key in ("steps", "distance", "cals"):
            if key in activity_type_obj and activity_type_obj[key] is not None:
                value = activity_type_obj[key]
                if not isinstance(value, (int, float)) or isinstance(value, bool):
                    issues.append(
                        InvariantIssue(
                            level="error",
                            code=f"{key}_not_numeric",
                            message=f"{key} must be numeric when present, got {value!r}",
                            record_index=idx,
                        )
                    )

    return issues


def _check_generic(outputs: list[dict[str, Any]]) -> list[InvariantIssue]:
    # No-op generic checker for tasks without task-specific invariants yet.
    return []


def check_invariants(task_id: str, outputs: list[dict[str, Any]]) -> InvariantReport:
    if task_id == "GB_004":
        issues = _check_gb004(outputs)
    elif task_id == "GB_007":
        issues = _check_gb007(outputs)
    else:
        issues = _check_generic(outputs)

    ok = not any(issue.level == "error" for issue in issues)
    return InvariantReport(ok=ok, issues=issues)


def check_invariants_from_file(task_id: str, outputs_path: str | Path) -> InvariantReport:
    outputs = _load_json(outputs_path)
    if not isinstance(outputs, list):
        return InvariantReport(
            ok=False,
            issues=[
                InvariantIssue(
                    level="error",
                    code="outputs_not_list",
                    message="Executed outputs file must contain a list of output records.",
                    record_index=None,
                )
            ],
        )
    return check_invariants(task_id, outputs)


def save_report(report: InvariantReport, output_path: str | Path) -> Path:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "ok": report.ok,
        "issues": [asdict(issue) for issue in report.issues],
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    return output_path