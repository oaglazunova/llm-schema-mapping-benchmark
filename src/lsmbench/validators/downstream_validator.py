from __future__ import annotations

from datetime import datetime
from typing import Any

from lsmbench.validators.execution_validator import validate_execution


def _is_date_string(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    if len(value) != 10:
        return False
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def _is_datetime_string(value: Any) -> bool:
    if not isinstance(value, str):
        return False

    candidate = value
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"

    try:
        datetime.fromisoformat(candidate)
        return True
    except ValueError:
        return False


def _matches_type_kind(value: Any, kind: str) -> bool:
    if kind == "string":
        return isinstance(value, str)
    if kind == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if kind == "number":
        return (isinstance(value, int) or isinstance(value, float)) and not isinstance(value, bool)
    if kind == "boolean":
        return isinstance(value, bool)
    if kind == "string-or-null":
        return value is None or isinstance(value, str)
    if kind == "integer-or-null":
        return value is None or (isinstance(value, int) and not isinstance(value, bool))
    if kind == "date-string":
        return _is_date_string(value)
    if kind == "datetime-string":
        return _is_datetime_string(value)

    raise ValueError(f"Unknown downstream field type kind: {kind}")


def _check_record_count_matches_expected(
    produced_records: list[dict[str, Any]],
    expected_records: list[dict[str, Any]],
) -> list[str]:
    if len(produced_records) != len(expected_records):
        return [
            f"Produced record count {len(produced_records)} does not match expected count {len(expected_records)}"
        ]
    return []


def _check_required_fields_present(
    produced_records: list[dict[str, Any]],
    fields: list[str],
    scope: str,
) -> list[str]:
    errors: list[str] = []

    if scope == "any_record":
        if any(all(record.get(field) not in (None, "", []) for field in fields) for record in produced_records):
            return []
        return [f"No record satisfies required_fields_present for fields {fields!r}"]

    # default: all_records
    for i, record in enumerate(produced_records):
        for field in fields:
            if record.get(field) in (None, "", []):
                errors.append(f"record[{i}] missing required downstream field '{field}'")

    return errors


def _check_field_types(
    produced_records: list[dict[str, Any]],
    fields: dict[str, str],
    scope: str,
) -> list[str]:
    errors: list[str] = []

    if scope == "any_record":
        for record in produced_records:
            if all(_matches_type_kind(record.get(field), kind) for field, kind in fields.items()):
                return []
        return [f"No record satisfies downstream field type contract {fields!r}"]

    # default: all_records
    for i, record in enumerate(produced_records):
        for field, kind in fields.items():
            value = record.get(field)
            if not _matches_type_kind(value, kind):
                errors.append(
                    f"record[{i}] field '{field}' has value {value!r}, expected downstream type '{kind}'"
                )

    return errors


def validate_downstream(task: dict[str, Any], plan: dict[str, Any]) -> dict[str, Any]:
    """
    Downstream validation runs only after execution validation succeeds.
    """
    exec_report = validate_execution(task, plan)
    if not exec_report["valid"]:
        return {
            "valid": False,
            "errors": [
                "Execution validation failed; downstream validation not attempted."
            ] + exec_report["errors"],
            "execution_report": exec_report,
        }

    produced_records = exec_report["produced"]["records"]
    expected_records = exec_report["expected"]["records"]

    checks = task.get("downstream_checks", [])
    errors: list[str] = []

    for idx, check in enumerate(checks):
        check_type = check["type"]
        scope = check.get("scope", "all_records")

        if check_type == "record_count_matches_expected":
            errors.extend(_check_record_count_matches_expected(produced_records, expected_records))

        elif check_type == "required_fields_present":
            fields = check["fields"]
            if not isinstance(fields, list):
                errors.append(f"downstream_checks[{idx}] expected list under 'fields'")
                continue
            errors.extend(_check_required_fields_present(produced_records, fields, scope))

        elif check_type == "field_types":
            fields = check["fields"]
            if not isinstance(fields, dict):
                errors.append(f"downstream_checks[{idx}] expected dict under 'fields'")
                continue
            errors.extend(_check_field_types(produced_records, fields, scope))

        else:
            errors.append(f"Unknown downstream check type: {check_type}")

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "execution_report": exec_report,
    }