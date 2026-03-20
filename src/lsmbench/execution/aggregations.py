from __future__ import annotations

from typing import Any

from lsmbench.execution.json_path import resolve_all


def _apply_aggregation_to_values(function: str, values: list[Any]) -> Any:
    if function == "count":
        return len(values)

    if function == "sum":
        return sum(float(v) for v in values if v is not None)

    if function == "avg":
        nums = [float(v) for v in values if v is not None]
        return None if not nums else sum(nums) / len(nums)

    if function == "min":
        nums = [v for v in values if v is not None]
        return None if not nums else min(nums)

    if function == "max":
        nums = [v for v in values if v is not None]
        return None if not nums else max(nums)

    if function == "latest":
        return None if not values else values[-1]

    if function == "count_true":
        return sum(1 for v in values if v is True)

    if function == "all_true":
        return all(v is True for v in values)

    if function == "distinct_count":
        normalized = [repr(v) for v in values]
        return len(set(normalized))

    if function == "first":
        return None if not values else values[0]

    raise NotImplementedError(f"Unsupported aggregation: {function}")


def apply_aggregation(
    function: str,
    context: dict[str, Any],
    source_path: str | None = None,
    source_paths: list[str] | None = None,
) -> Any:
    values: list[Any] = []

    if source_path is not None:
        values = resolve_all(context, source_path)
    elif source_paths is not None:
        for path in source_paths:
            values.extend(resolve_all(context, path))

    return _apply_aggregation_to_values(function, values)


def apply_aggregation_to_records(
    function: str,
    records: list[dict[str, Any]],
    source_path: str | None = None,
    source_paths: list[str] | None = None,
) -> Any:
    values: list[Any] = []

    if source_path is not None:
        for record in records:
            values.extend(resolve_all(record, source_path))

    elif source_paths is not None:
        for record in records:
            for path in source_paths:
                values.extend(resolve_all(record, path))

    return _apply_aggregation_to_values(function, values)