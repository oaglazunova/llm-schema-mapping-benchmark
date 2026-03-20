from __future__ import annotations

from typing import Any

from jsonschema import Draft202012Validator

from lsmbench.benchmark.task_loader import load_fixture, load_gold
from lsmbench.execution.engine import execute_plan_on_fixture


def _validate_records_against_target_schema(
    records: list[dict[str, Any]],
    target_schema: dict[str, Any],
) -> list[str]:
    validator = Draft202012Validator(target_schema)
    errors: list[str] = []

    for i, record in enumerate(records):
        for err in validator.iter_errors(record):
            path = "$"
            if err.path:
                path += "".join(
                    f"[{p}]" if isinstance(p, int) else f".{p}"
                    for p in err.path
                )
            errors.append(f"record[{i}]{path}: {err.message}")

    return errors


def validate_execution(task: dict[str, Any], plan: dict[str, Any]) -> dict[str, Any]:
    input_fixture = load_fixture(task, "input")
    expected_fixture = load_fixture(task, "expected")

    produced = execute_plan_on_fixture(input_fixture, plan)

    produced_records = produced.get("records", [])
    expected_records = expected_fixture.get("records", [])

    target_schema_errors = _validate_records_against_target_schema(
        produced_records,
        task["target_schema"],
    )

    exact_match = produced_records == expected_records

    errors: list[str] = []
    if target_schema_errors:
        errors.extend(target_schema_errors)

    if not exact_match:
        errors.append(
            f"Produced output does not exactly match expected fixture. "
            f"Produced={produced_records!r} Expected={expected_records!r}"
        )

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "produced": produced,
        "expected": expected_fixture,
    }


def validate_execution_for_task(task: dict[str, Any]) -> dict[str, Any]:
    plan = load_gold(task, "plan")
    return validate_execution(task, plan)