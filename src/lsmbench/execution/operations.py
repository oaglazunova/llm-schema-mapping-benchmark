from __future__ import annotations

import ast
import json
from datetime import datetime
from typing import Any

from lsmbench.execution.json_path import resolve_one, get_nested


def _to_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _parse_datetime_string(value: Any) -> str:
    s = _to_str(value).strip()
    if not s:
        raise ValueError("Cannot parse empty datetime string")

    # Handle common formats used in the current benchmark.
    if s.endswith("Z"):
        return s

    # "2025-10-04 16:12:08" -> ISO-like
    if " " in s and "T" not in s:
        return s.replace(" ", "T")

    return s


def _parse_date_string(value: Any) -> str:
    s = _to_str(value).strip()
    if not s:
        raise ValueError("Cannot parse empty date string")
    return s[:10]


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    s = _to_str(value).strip().lower()
    if s in {"true", "1", "yes"}:
        return True
    if s in {"false", "0", "no"}:
        return False
    raise ValueError(f"Cannot cast to boolean: {value!r}")


def _extract_kv(items: Any, key: str, delimiter: str = ":") -> Any:
    if not isinstance(items, list):
        return None

    prefix = f"{key}{delimiter}"
    for item in items:
        if isinstance(item, str) and item.startswith(prefix):
            return item[len(prefix):]
    return None


def _extract_array_field(
    arr: Any,
    *,
    match_field: str,
    match_value: Any,
    value_field: str | None = None,
    nested_array_field: str | None = None,
    nested_index: int | None = None,
) -> Any:
    if not isinstance(arr, list):
        return None

    for item in arr:
        if not isinstance(item, dict):
            continue

        candidate = get_nested(item, match_field)
        if candidate != match_value:
            continue

        current = item

        if nested_array_field is not None:
            nested = get_nested(current, nested_array_field)
            if not isinstance(nested, list):
                return None
            idx = 0 if nested_index is None else nested_index
            if idx < 0 or idx >= len(nested):
                return None
            current = nested[idx]

        if value_field is None:
            return current

        return get_nested(current, value_field)

    return None


def _parse_json_array_map_fields(value: Any, field_map: dict[str, str]) -> list[dict[str, Any]]:
    """
    Parse a JSON-encoded array of objects and remap object keys.

    Example:
        field_map = {
            "tk": "code",
            "condition": "condition_text",
            "accepted": "accepted",
        }

    Input:
        '[{"tk":"data-sharing","condition":"Consent text","accepted":true}]'

    Output:
        [{"code":"data-sharing","condition_text":"Consent text","accepted":True}]
    """
    parsed = json.loads(_to_str(value))

    if not isinstance(parsed, list):
        raise ValueError("parse_json_array_map_fields expected a JSON array")

    out: list[dict[str, Any]] = []

    for idx, item in enumerate(parsed):
        if not isinstance(item, dict):
            raise ValueError(
                f"parse_json_array_map_fields expected array of objects, "
                f"but item {idx} is {type(item).__name__}"
            )

        remapped: dict[str, Any] = {}
        for src_key, target_key in field_map.items():
            remapped[target_key] = item.get(src_key)

        out.append(remapped)

    return out


def apply_operation(
    operation: str,
    context: dict[str, Any],
    source_paths: list[str],
    parameters: dict[str, Any] | None = None,
) -> Any:
    params = parameters or {}
    values = [resolve_one(context, path) for path in source_paths]

    if operation in {"copy", "rename"}:
        return values[0] if values else None

    if operation == "cast_string":
        return _to_str(values[0])

    if operation == "cast_integer":
        return int(values[0])

    if operation == "cast_number":
        return float(values[0])

    if operation == "cast_boolean":
        return _to_bool(values[0])

    if operation == "parse_date":
        return _parse_date_string(values[0])

    if operation == "parse_datetime":
        return _parse_datetime_string(values[0])

    if operation == "truncate_date":
        return _parse_date_string(values[0])

    if operation == "normalize_enum":
        value = values[0]
        mapping = params.get("mapping")
        if isinstance(mapping, dict):
            return mapping.get(value, value)
        return value

    if operation == "normalize_boolean":
        return _to_bool(values[0])

    if operation == "concat":
        sep = params.get("separator", "")
        return sep.join(_to_str(v) for v in values if v is not None)

    if operation == "split":
        delimiter = params.get("delimiter", ",")
        return _to_str(values[0]).split(delimiter)

    if operation == "derive_arithmetic":
        op = params.get("op")
        if op == "add":
            return sum(float(v) for v in values if v is not None)
        raise NotImplementedError(f"Unsupported derive_arithmetic op: {op}")

    if operation == "default_value":
        return params.get("value")

    if operation == "coalesce":
        for v in values:
            if v not in (None, "", []):
                return v
        return None

    if operation == "latest_value":
        if not values:
            return None
        if isinstance(values[0], list) and values[0]:
            return values[0][-1]
        return values[-1]

    if operation == "parse_json_array":
        parsed = json.loads(_to_str(values[0]))
        if not isinstance(parsed, list):
            raise ValueError("parse_json_array expected a JSON array")
        return parsed

    if operation == "parse_json_object":
        parsed = json.loads(_to_str(values[0]))
        if not isinstance(parsed, dict):
            raise ValueError("parse_json_object expected a JSON object")
        return parsed

    if operation == "parse_pythonish_object":
        parsed = ast.literal_eval(_to_str(values[0]))
        if not isinstance(parsed, dict):
            raise ValueError("parse_pythonish_object expected a dict-like object")
        return parsed

    if operation == "extract_kv_value":
        key = params["key"]
        delimiter = params.get("delimiter", ":")
        return _extract_kv(values[0], key=key, delimiter=delimiter)

    if operation == "extract_kv_value_cast_integer":
        key = params["key"]
        delimiter = params.get("delimiter", ":")
        out = _extract_kv(values[0], key=key, delimiter=delimiter)
        return None if out is None else int(out)

    if operation == "extract_object_field":
        field = params["field"]
        obj = values[0]
        if not isinstance(obj, dict):
            return None
        return get_nested(obj, field)

    if operation == "extract_array_field":
        return _extract_array_field(
            values[0],
            match_field=params["match_field"],
            match_value=params["match_value"],
            value_field=params.get("value_field"),
            nested_array_field=params.get("nested_array_field"),
            nested_index=params.get("nested_index"),
        )

    if operation == "parse_json_array_map_fields":
        field_map = params.get("field_map")
        if not isinstance(field_map, dict) or not field_map:
            raise ValueError(
                "parse_json_array_map_fields requires parameters['field_map'] "
                "to be a non-empty dict"
            )
        return _parse_json_array_map_fields(values[0], field_map=field_map)

    raise NotImplementedError(f"Unsupported operation: {operation}")