from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator


@dataclass
class ValidationIssue:
    level: str
    code: str
    message: str
    file: str


@dataclass
class ValidationReport:
    ok: bool
    issues: list[ValidationIssue]


def _load_json(path: Path) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _load_schema_validator(schema_path: Path) -> Draft202012Validator:
    schema = _load_json(schema_path)
    return Draft202012Validator(schema)


def _jsonschema_issues(payload: Any, validator: Draft202012Validator, file_path: Path) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    for err in sorted(validator.iter_errors(payload), key=lambda e: list(e.path)):
        path_str = ".".join(str(x) for x in err.path) if err.path else "<root>"
        issues.append(
            ValidationIssue(
                level="error",
                code="jsonschema",
                message=f"{path_str}: {err.message}",
                file=str(file_path),
            )
        )
    return issues


def _extract_source_fields(task_payload: dict[str, Any]) -> set[str]:
    fields = task_payload.get("source_schema", {}).get("fields", [])
    return {f["name"] for f in fields if isinstance(f, dict) and "name" in f}


def _extract_target_fields(task_payload: dict[str, Any]) -> set[str]:
    fields = task_payload.get("target_schema", {}).get("fields", [])
    return {f["name"] for f in fields if isinstance(f, dict) and "name" in f}


def _validate_gold_plan_against_task(
    task_payload: dict[str, Any],
    plan_payload: dict[str, Any],
    plan_path: Path,
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []

    source_fields = _extract_source_fields(task_payload)
    target_fields = _extract_target_fields(task_payload)

    if plan_payload.get("task_id") != task_payload.get("task_id"):
        issues.append(
            ValidationIssue(
                level="error",
                code="task_id_mismatch",
                message=f"Plan task_id={plan_payload.get('task_id')} does not match task task_id={task_payload.get('task_id')}",
                file=str(plan_path),
            )
        )

    if plan_payload.get("target_entity") != task_payload.get("target_entity"):
        issues.append(
            ValidationIssue(
                level="error",
                code="target_entity_mismatch",
                message=f"Plan target_entity={plan_payload.get('target_entity')} does not match task target_entity={task_payload.get('target_entity')}",
                file=str(plan_path),
            )
        )

    for i, mapping in enumerate(plan_payload.get("field_mappings", [])):
        target_field = mapping.get("target_field")
        source_paths = mapping.get("source_paths", [])

        if target_field not in target_fields:
            issues.append(
                ValidationIssue(
                    level="error",
                    code="unknown_target_field",
                    message=f"field_mappings[{i}].target_field={target_field!r} not found in task target_schema",
                    file=str(plan_path),
                )
            )

        for sp in source_paths:
            if isinstance(sp, str) and sp.startswith("$."):
                candidate = sp[2:]
                # Only validate simple direct paths here; leave complex paths for execution validator later.
                if "[" not in candidate and "." not in candidate and candidate not in source_fields:
                    issues.append(
                        ValidationIssue(
                            level="warning",
                            code="unknown_source_field",
                            message=f"field_mappings[{i}].source_paths contains {sp!r}, which is not a known top-level source field",
                            file=str(plan_path),
                        )
                    )

    for i, agg in enumerate(plan_payload.get("aggregations", [])):
        target_field = agg.get("target_field")
        if target_field not in target_fields:
            issues.append(
                ValidationIssue(
                    level="error",
                    code="unknown_aggregation_target_field",
                    message=f"aggregations[{i}].target_field={target_field!r} not found in task target_schema",
                    file=str(plan_path),
                )
            )

    return issues


def validate_task_bundle(
    task_path: str | Path,
    plan_path: str | Path,
    task_schema_path: str | Path,
    plan_schema_path: str | Path,
) -> ValidationReport:
    task_path = Path(task_path)
    plan_path = Path(plan_path)
    task_schema_path = Path(task_schema_path)
    plan_schema_path = Path(plan_schema_path)

    task_payload = _load_json(task_path)
    plan_payload = _load_json(plan_path)

    task_validator = _load_schema_validator(task_schema_path)
    plan_validator = _load_schema_validator(plan_schema_path)

    issues: list[ValidationIssue] = []
    issues.extend(_jsonschema_issues(task_payload, task_validator, task_path))
    issues.extend(_jsonschema_issues(plan_payload, plan_validator, plan_path))
    issues.extend(_validate_gold_plan_against_task(task_payload, plan_payload, plan_path))

    ok = not any(issue.level == "error" for issue in issues)
    return ValidationReport(ok=ok, issues=issues)


def save_report(report: ValidationReport, output_path: str | Path) -> Path:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "ok": report.ok,
        "issues": [asdict(issue) for issue in report.issues],
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    return output_path