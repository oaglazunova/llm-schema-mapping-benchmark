from __future__ import annotations

from typing import Any

from lsmbench.benchmark.task_loader import load_fixture, load_gold
from lsmbench.execution.engine import execute_plan_on_fixture


def _matches_field_type(value: Any, type_name: str) -> bool:
    if value is None:
        return True

    if type_name == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if type_name == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if type_name == "string":
        return isinstance(value, str)
    if type_name == "boolean":
        return isinstance(value, bool)
    if type_name == "array":
        return isinstance(value, list)
    if type_name == "object":
        return isinstance(value, dict)
    if type_name == "date":
        return isinstance(value, str) and len(value) == 10
    if type_name == "datetime":
        return isinstance(value, str) and len(value) >= 19

    return True


def _validate_records_against_target_schema(
    records: list[dict[str, Any]],
    target_schema: dict[str, Any],
) -> list[str]:
    errors: list[str] = []
    fields = target_schema.get("fields", [])

    for i, record in enumerate(records):
        for field in fields:
            name = field["name"]
            typ = field["type"]
            required = field.get("required", False)

            if required and name not in record:
                errors.append(f"record[{i}].{name}: missing required field")
                continue

            value = record.get(name)
            if not _matches_field_type(value, typ):
                errors.append(
                    f"record[{i}].{name}: value {value!r} does not match target type {typ!r}"
                )

    return errors


def _fixture_records(obj):
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict) and "records" in obj:
        return obj["records"]
    if isinstance(obj, dict):
        return [obj]
    raise ValueError(f"Unsupported fixture object: {type(obj)!r}")


def validate_execution(task: dict[str, Any], plan: dict[str, Any]) -> dict[str, Any]:
    input_fixture = load_fixture(task, "input")
    expected_fixture = load_fixture(task, "expected")

    produced = execute_plan_on_fixture(input_fixture, plan, task_payload=task)

    produced_records = _fixture_records(produced)
    expected_records = _fixture_records(expected_fixture)

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