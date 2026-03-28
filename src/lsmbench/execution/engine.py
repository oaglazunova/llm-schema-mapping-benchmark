from __future__ import annotations

import ast
import json
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass
class ExecutionIssue:
    level: str
    code: str
    message: str
    record_index: int | None = None


@dataclass
class ExecutionResult:
    ok: bool
    outputs: list[dict[str, Any]]
    issues: list[ExecutionIssue]


def _load_json(path: str | Path) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _parse_datetime(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if not isinstance(value, str):
        raise ValueError(f"Expected datetime string, got {type(value).__name__}")
    value = value.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(value, fmt)
            return dt.isoformat(sep=" ")
        except ValueError:
            continue
    raise ValueError(f"Could not parse datetime: {value!r}")


def _truncate_date(value: Any) -> str | None:
    if value is None:
        return None
    dt = _parse_datetime(value)
    return dt[:10] if dt else None


def _cast_integer(value: Any) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise ValueError("Cannot cast bool to integer")
    return int(value)


def _cast_number(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise ValueError("Cannot cast bool to number")
    return float(value)


def _cast_boolean(value: Any) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        s = value.strip().lower()
        if s in {"true", "1", "yes"}:
            return True
        if s in {"false", "0", "no"}:
            return False
    raise ValueError(f"Cannot cast to boolean: {value!r}")


def _parse_json_array(value: Any) -> list[Any] | None:
    if value is None or value == "":
        return None
    if isinstance(value, list):
        return value
    if not isinstance(value, str):
        raise ValueError(f"Expected JSON array string, got {type(value).__name__}")
    parsed = json.loads(value)
    if not isinstance(parsed, list):
        raise ValueError("Parsed value is not a list")
    return parsed


def _parse_json_object(value: Any) -> dict[str, Any] | None:
    if value is None or value == "":
        return None
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        raise ValueError(f"Expected JSON object string, got {type(value).__name__}")
    parsed = json.loads(value)
    if not isinstance(parsed, dict):
        raise ValueError("Parsed value is not an object")
    return parsed


def _parse_pythonish_object(value: Any) -> dict[str, Any] | None:
    if value is None or value == "":
        return None
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        raise ValueError(f"Expected Python-like object string, got {type(value).__name__}")
    parsed = ast.literal_eval(value)
    if not isinstance(parsed, dict):
        raise ValueError("Parsed value is not an object")
    return parsed


def _extract_kv_value(value: Any, *, key: str, delimiter: str = ":") -> str | None:
    if value is None:
        return None
    if not isinstance(value, list):
        raise ValueError(f"Expected list of key:value strings, got {type(value).__name__}")
    for item in value:
        if isinstance(item, str) and delimiter in item:
            left, right = item.split(delimiter, 1)
            if left.strip() == key:
                return right.strip()
    return None


def _extract_kv_value_cast_integer(value: Any, *, key: str, delimiter: str = ":") -> int | None:
    raw = _extract_kv_value(value, key=key, delimiter=delimiter)
    if raw is None:
        return None
    return _cast_integer(raw)


def _copy(value: Any) -> Any:
    return deepcopy(value)


def _get_source_value(record: dict[str, Any], source_path: str) -> Any:
    """
    Minimal source-path support for v1:
    - $.FIELD
    """
    if not source_path.startswith("$."):
        raise ValueError(f"Unsupported source path: {source_path}")
    field = source_path[2:]
    if "." in field or "[" in field:
        raise ValueError(f"Complex source paths not supported yet: {source_path}")
    return record.get(field)


def _get_output_path_value(output_obj: dict[str, Any], path: str) -> Any:
    """
    Minimal output-path support for v1 aggregations:
    - $.field
    - $.field[*]
    - $.field[*].subfield
    """
    if not path.startswith("$."):
        raise ValueError(f"Unsupported output path: {path}")

    expr = path[2:]

    if "[*]." in expr:
        field, subfield = expr.split("[*].", 1)
        arr = output_obj.get(field, [])
        if arr is None:
            return []
        if not isinstance(arr, list):
            raise ValueError(f"Expected list at {field}, got {type(arr).__name__}")
        return [item.get(subfield) for item in arr if isinstance(item, dict)]

    if expr.endswith("[*]"):
        field = expr[:-3]
        arr = output_obj.get(field, [])
        if arr is None:
            return []
        if not isinstance(arr, list):
            raise ValueError(f"Expected list at {field}, got {type(arr).__name__}")
        return arr

    return output_obj.get(expr)


def _apply_operation(operation: str, value: Any, parameters: dict[str, Any] | None = None) -> Any:
    parameters = parameters or {}

    if operation in {"copy", "rename", "latest_value"}:
        return _copy(value)
    if operation == "cast_string":
        return None if value is None else str(value)
    if operation == "cast_integer":
        return _cast_integer(value)
    if operation == "cast_number":
        return _cast_number(value)
    if operation == "cast_boolean":
        return _cast_boolean(value)
    if operation == "parse_datetime":
        return _parse_datetime(value)
    if operation == "parse_date":
        return _truncate_date(value)
    if operation == "truncate_date":
        return _truncate_date(value)
    if operation == "normalize_enum":
        return _copy(value)
    if operation == "normalize_boolean":
        return _cast_boolean(value)
    if operation == "default_value":
        return parameters.get("value") if value in {None, ""} else value
    if operation == "coalesce":
        return value if value not in {None, ""} else parameters.get("default")
    if operation == "parse_json_array":
        return _parse_json_array(value)
    if operation == "parse_json_object":
        return _parse_json_object(value)
    if operation == "parse_pythonish_object":
        return _parse_pythonish_object(value)
    if operation == "extract_kv_value":
        return _extract_kv_value(value, key=parameters["key"], delimiter=parameters.get("delimiter", ":"))
    if operation == "extract_kv_value_cast_integer":
        return _extract_kv_value_cast_integer(value, key=parameters["key"], delimiter=parameters.get("delimiter", ":"))

    raise ValueError(f"Unsupported operation: {operation}")


def _apply_aggregation(function: str, values: Any) -> Any:
    if function == "count":
        if values is None:
            return 0
        if isinstance(values, list):
            return len(values)
        return 1
    if function == "sum":
        return sum(v for v in values if v is not None)
    if function == "avg":
        nums = [v for v in values if v is not None]
        return None if not nums else sum(nums) / len(nums)
    if function == "min":
        nums = [v for v in values if v is not None]
        return None if not nums else min(nums)
    if function == "max":
        nums = [v for v in values if v is not None]
        return None if not nums else max(nums)
    if function == "latest":
        if isinstance(values, list) and values:
            return values[-1]
        return values
    if function == "first":
        if isinstance(values, list) and values:
            return values[0]
        return values
    if function == "count_true":
        if not isinstance(values, list):
            raise ValueError("count_true expects a list")
        return sum(1 for v in values if v is True)
    if function == "all_true":
        if not isinstance(values, list):
            raise ValueError("all_true expects a list")
        return all(v is True for v in values)
    if function == "distinct_count":
        if not isinstance(values, list):
            raise ValueError("distinct_count expects a list")
        return len({json.dumps(v, sort_keys=True, default=str) for v in values})

    raise ValueError(f"Unsupported aggregation: {function}")


def _validate_output_record(output_record: dict[str, Any], target_schema: dict[str, Any], record_index: int) -> list[ExecutionIssue]:
    issues: list[ExecutionIssue] = []

    fields = target_schema.get("fields", [])
    for field in fields:
        name = field["name"]
        typ = field["type"]
        required = field.get("required", False)

        if required and name not in output_record:
            issues.append(
                ExecutionIssue(
                    level="error",
                    code="missing_required_field",
                    message=f"Missing required field {name!r}",
                    record_index=record_index,
                )
            )
            continue

        value = output_record.get(name)
        if value is None:
            continue

        valid = True
        if typ == "integer":
            valid = isinstance(value, int) and not isinstance(value, bool)
        elif typ == "number":
            valid = isinstance(value, (int, float)) and not isinstance(value, bool)
        elif typ == "string":
            valid = isinstance(value, str)
        elif typ == "boolean":
            valid = isinstance(value, bool)
        elif typ == "array":
            valid = isinstance(value, list)
        elif typ == "object":
            valid = isinstance(value, dict)
        elif typ == "date":
            valid = isinstance(value, str) and len(value) == 10
        elif typ == "datetime":
            valid = isinstance(value, str) and len(value) >= 19

        if not valid:
            issues.append(
                ExecutionIssue(
                    level="error",
                    code="type_mismatch",
                    message=f"Field {name!r} expected type {typ!r}, got value {value!r}",
                    record_index=record_index,
                )
            )

    return issues


def execute_plan_on_records(
    task_payload: dict[str, Any],
    plan_payload: dict[str, Any],
    records: list[dict[str, Any]],
) -> ExecutionResult:
    outputs: list[dict[str, Any]] = []
    issues: list[ExecutionIssue] = []
    target_schema = task_payload["target_schema"]

    for idx, record in enumerate(records):
        try:
            output_obj: dict[str, Any] = {}

            # Field mappings
            for mapping in plan_payload.get("field_mappings", []):
                target_field = mapping["target_field"]
                operation = mapping["operation"]
                source_paths = mapping.get("source_paths", [])
                parameters = mapping.get("parameters", {})

                value = None
                if source_paths:
                    value = _get_source_value(record, source_paths[0])

                output_obj[target_field] = _apply_operation(operation, value, parameters)

            # Aggregations over interim output
            for agg in plan_payload.get("aggregations", []):
                target_field = agg["target_field"]
                function = agg["function"]

                if "source_path" in agg:
                    values = _get_output_path_value(output_obj, agg["source_path"])
                else:
                    values = [_get_output_path_value(output_obj, p) for p in agg.get("source_paths", [])]

                output_obj[target_field] = _apply_aggregation(function, values)

            outputs.append(output_obj)
            issues.extend(_validate_output_record(output_obj, target_schema, idx))

        except Exception as e:
            issues.append(
                ExecutionIssue(
                    level="error",
                    code="execution_error",
                    message=str(e),
                    record_index=idx,
                )
            )

    ok = not any(issue.level == "error" for issue in issues)
    return ExecutionResult(ok=ok, outputs=outputs, issues=issues)


def execute_plan_from_files(
    task_path: str | Path,
    plan_path: str | Path,
    fixture_input_path: str | Path,
) -> ExecutionResult:
    task_payload = _load_json(task_path)
    plan_payload = _load_json(plan_path)
    records = _load_json(fixture_input_path)

    if not isinstance(records, list):
        raise ValueError("Fixture input must be a list of records")

    return execute_plan_on_records(task_payload, plan_payload, records)