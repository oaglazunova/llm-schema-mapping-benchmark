from __future__ import annotations

from typing import Any

from lsmbench.execution.json_path import resolve_one, tokenize_json_path, get_nested


def _alias_from_path(path: str) -> str:
    """
    Use the last property token in the JSONPath as the alias.

    Examples:
    - $.orders -> orders
    - $.customers -> customers
    """
    tokens = tokenize_json_path(path)
    prop_tokens = [value for kind, value in tokens if kind == "prop"]
    if not prop_tokens:
        raise ValueError(f"Could not derive alias from path: {path}")
    return prop_tokens[-1]


def _ensure_list(value: Any, path: str) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise ValueError(f"Join path '{path}' did not resolve to a list")
    out: list[dict[str, Any]] = []
    for i, item in enumerate(value):
        if not isinstance(item, dict):
            raise ValueError(f"Join path '{path}' item {i} is not an object")
        out.append(item)
    return out


def materialize_single_join(
    bundle_record: dict[str, Any],
    join_spec: dict[str, Any],
) -> list[dict[str, Any]]:
    """
    Materialize joined row contexts from one bundle record.

    Each output row looks like:
        {
          "<left_alias>": {...},
          "<right_alias>": {...} | None
        }
    """
    left_path = join_spec["left_path"]
    right_path = join_spec["right_path"]
    left_key = join_spec["left_key"]
    right_key = join_spec["right_key"]
    join_type = join_spec["join_type"]

    left_alias = _alias_from_path(left_path)
    right_alias = _alias_from_path(right_path)

    left_rows = _ensure_list(resolve_one(bundle_record, left_path), left_path)
    right_rows = _ensure_list(resolve_one(bundle_record, right_path), right_path)

    # Build simple hash index on the right side
    right_index: dict[Any, list[dict[str, Any]]] = {}
    for row in right_rows:
        key = get_nested(row, right_key)
        right_index.setdefault(key, []).append(row)

    out: list[dict[str, Any]] = []

    for left_row in left_rows:
        key = get_nested(left_row, left_key)
        matches = right_index.get(key, [])

        if matches:
            for right_row in matches:
                out.append({
                    left_alias: left_row,
                    right_alias: right_row,
                })
        elif join_type == "left":
            out.append({
                left_alias: left_row,
                right_alias: None,
            })
        elif join_type == "inner":
            continue
        else:
            raise NotImplementedError(f"Unsupported join type: {join_type}")

    return out