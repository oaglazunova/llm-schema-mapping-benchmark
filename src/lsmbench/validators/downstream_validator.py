from __future__ import annotations

from datetime import datetime
from typing import Any

from lsmbench.validators.execution_validator import validate_execution


def _fixture_records(obj: Any) -> list[dict[str, Any]]:
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict) and "records" in obj:
        return obj["records"]
    if isinstance(obj, dict):
        return [obj]
    raise ValueError(f"Unsupported fixture/report payload type: {type(obj)!r}")


def _check_record_count_matches_expected(
    produced_records: list[dict[str, Any]],
    expected_records: list[dict[str, Any]],
) -> list[str]:
    if len(produced_records) != len(expected_records):
        return [
            f"Record count mismatch: produced={len(produced_records)} expected={len(expected_records)}"
        ]
    return []


def _check_required_fields_present(
    produced_records: list[dict[str, Any]],
    fields: list[str],
    *,
    scope: str = "all_records",
) -> list[str]:
    errors: list[str] = []

    if scope not in {"all_records", "any_record"}:
        errors.append(f"Unsupported downstream check scope: {scope!r}")
        return errors

    if scope == "all_records":
        for i, rec in enumerate(produced_records):
            for field in fields:
                if field not in rec:
                    errors.append(f"produced[{i}] missing required downstream field {field!r}")
        return errors

    # scope == "any_record"
    for field in fields:
        if not any(field in rec for rec in produced_records):
            errors.append(f"No produced record contains required downstream field {field!r}")

    return errors


def _is_date_string(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 10
        and value[4] == "-"
        and value[7] == "-"
    )


def _is_datetime_string(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False


def _value_matches_type(value: Any, expected: str) -> bool:
    if expected == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if expected == "string":
        return isinstance(value, str)
    if expected == "boolean":
        return isinstance(value, bool)
    if expected == "array":
        return isinstance(value, list)
    if expected == "object":
        return isinstance(value, dict)
    if expected == "date-string":
        return _is_date_string(value)
    if expected == "datetime-string":
        return _is_datetime_string(value)
    if expected == "integer-or-null":
        return value is None or (isinstance(value, int) and not isinstance(value, bool))
    if expected == "string-or-null":
        return value is None or isinstance(value, str)
    # Unknown expected type should fail closed in downstream checks.
    return False


def _check_field_types(
    produced_records: list[dict[str, Any]],
    fields: dict[str, str],
    *,
    scope: str = "all_records",
) -> list[str]:
    errors: list[str] = []

    if scope not in {"all_records", "any_record"}:
        errors.append(f"Unsupported downstream check scope: {scope!r}")
        return errors

    if scope == "all_records":
        for i, rec in enumerate(produced_records):
            for field, expected_type in fields.items():
                if field not in rec:
                    errors.append(f"produced[{i}] missing field {field!r} for type check")
                    continue
                value = rec[field]
                if not _value_matches_type(value, expected_type):
                    errors.append(
                        f"produced[{i}].{field}={value!r} does not match expected type {expected_type!r}"
                    )
        return errors

    # scope == "any_record"
    for field, expected_type in fields.items():
        matched = False
        for rec in produced_records:
            if field in rec and _value_matches_type(rec[field], expected_type):
                matched = True
                break
        if not matched:
            errors.append(
                f"No produced record contains field {field!r} matching expected type {expected_type!r}"
            )

    return errors


def validate_downstream(task: dict[str, Any], plan: dict[str, Any]) -> dict[str, Any]:
    exec_report = validate_execution(task, plan)

    if not exec_report["valid"]:
        return {
            "valid": False,
            "errors": [
                "Execution validation failed; downstream validation not attempted.",
                *exec_report["errors"],
            ],
        }

    produced_records = _fixture_records(exec_report["produced"])
    expected_records = _fixture_records(exec_report["expected"])

    checks = task.get("downstream_checks", [])
    if not checks:
        return {"valid": True, "errors": []}

    errors: list[str] = []

    for check in checks:
        ctype = check["type"]

        if ctype == "record_count_matches_expected":
            errors.extend(
                _check_record_count_matches_expected(
                    produced_records,
                    expected_records,
                )
            )

        elif ctype == "required_fields_present":
            errors.extend(
                _check_required_fields_present(
                    produced_records,
                    check["fields"],
                    scope=check.get("scope", "all_records"),
                )
            )

        elif ctype == "field_types":
            fields = check.get("fields", {})
            if not isinstance(fields, dict):
                errors.append(
                    f"Downstream check 'field_types' expects an object mapping field->type, got {type(fields)!r}"
                )
            else:
                errors.extend(
                    _check_field_types(
                        produced_records,
                        fields,
                        scope=check.get("scope", "all_records"),
                    )
                )

        else:
            errors.append(f"Unknown downstream check type: {ctype!r}")

    return {
        "valid": len(errors) == 0,
        "errors": errors,
    }