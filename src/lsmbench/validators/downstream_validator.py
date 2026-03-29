from __future__ import annotations

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

    if scope != "all_records":
        errors.append(f"Unsupported downstream check scope: {scope!r}")
        return errors

    for i, rec in enumerate(produced_records):
        for field in fields:
            if field not in rec:
                errors.append(f"produced[{i}] missing required downstream field {field!r}")

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

        else:
            errors.append(f"Unknown downstream check type: {ctype!r}")

    return {
        "valid": len(errors) == 0,
        "errors": errors,
    }