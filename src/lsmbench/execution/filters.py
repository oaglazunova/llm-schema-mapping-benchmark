from __future__ import annotations

from typing import Any

from lsmbench.execution.json_path import resolve_all


def _compare_any(values: list[Any], predicate) -> bool:
    return any(predicate(v) for v in values)


def _compare_all(values: list[Any], predicate) -> bool:
    return all(predicate(v) for v in values)


def record_passes_filter(context: dict[str, Any], filt: dict[str, Any]) -> bool:
    """
    Evaluate one filter against one execution context.

    Supported operators:
    - eq
    - neq
    - gt
    - gte
    - lt
    - lte
    - in
    - not_in
    - exists
    - not_exists
    """
    path = filt["path"]
    operator = filt["operator"]
    value = filt.get("value")

    values = resolve_all(context, path)

    if operator == "exists":
        return len(values) > 0 and any(v is not None for v in values)

    if operator == "not_exists":
        return len(values) == 0 or all(v is None for v in values)

    if len(values) == 0:
        return False

    if operator == "eq":
        return _compare_any(values, lambda v: v == value)

    if operator == "neq":
        return _compare_all(values, lambda v: v != value)

    if operator == "gt":
        return _compare_any(values, lambda v: v is not None and v > value)

    if operator == "gte":
        return _compare_any(values, lambda v: v is not None and v >= value)

    if operator == "lt":
        return _compare_any(values, lambda v: v is not None and v < value)

    if operator == "lte":
        return _compare_any(values, lambda v: v is not None and v <= value)

    if operator == "in":
        if not isinstance(value, list):
            raise ValueError("'in' filter expects list value")
        return _compare_any(values, lambda v: v in value)

    if operator == "not_in":
        if not isinstance(value, list):
            raise ValueError("'not_in' filter expects list value")
        return _compare_all(values, lambda v: v not in value)

    raise NotImplementedError(f"Unsupported filter operator: {operator}")


def record_passes_filters(context: dict[str, Any], filters: list[dict[str, Any]]) -> bool:
    """
    AND semantics across filters.
    """
    return all(record_passes_filter(context, filt) for filt in filters)