from __future__ import annotations

from copy import deepcopy
from typing import Any

from lsmbench.execution.aggregations import apply_aggregation, apply_aggregation_to_records
from lsmbench.execution.filters import record_passes_filters
from lsmbench.execution.json_path import resolve_one
from lsmbench.execution.operations import apply_operation


def _build_group_key(record: dict[str, Any], group_by_paths: list[str]) -> tuple[Any, ...]:
    return tuple(resolve_one(record, path) for path in group_by_paths)


def execute_plan_on_record(
    record: dict[str, Any],
    plan: dict[str, Any],
) -> dict[str, Any] | None:
    """
    Execute one mapping plan against one source record.

    Current non-grouped scope:
    - field_mappings
    - filters
    - aggregations
    - no joins yet
    """
    if plan.get("joins"):
        raise NotImplementedError("Joins are not implemented in the execution engine yet.")

    context = deepcopy(record)
    output: dict[str, Any] = {}

    # field mappings
    for mapping in plan.get("field_mappings", []):
        value = apply_operation(
            operation=mapping["operation"],
            context=context,
            source_paths=mapping.get("source_paths", []),
            parameters=mapping.get("parameters"),
        )
        target_field = mapping["target_field"]
        output[target_field] = value
        context[target_field] = value

    # filters
    filters = plan.get("filters", [])
    if filters and not record_passes_filters(context, filters):
        return None

    # aggregations
    for agg in plan.get("aggregations", []):
        value = apply_aggregation(
            function=agg["function"],
            context=context,
            source_path=agg.get("source_path"),
            source_paths=agg.get("source_paths"),
        )
        target_field = agg["target_field"]
        output[target_field] = value
        context[target_field] = value

    return output


def execute_plan_on_group(
    records: list[dict[str, Any]],
    plan: dict[str, Any],
) -> dict[str, Any] | None:
    """
    Execute one grouped mapping plan against a group of source records.

    Grouped semantics:
    - field_mappings read from the first record in the group
    - aggregations read across the full group
    - filters run after mappings + aggregations on the enriched group context
    """
    if not records:
        return None

    if plan.get("joins"):
        raise NotImplementedError("Joins are not implemented in the execution engine yet.")

    first_record = records[0]
    context = deepcopy(first_record)
    output: dict[str, Any] = {}

    # field mappings from the first record
    for mapping in plan.get("field_mappings", []):
        value = apply_operation(
            operation=mapping["operation"],
            context=context,
            source_paths=mapping.get("source_paths", []),
            parameters=mapping.get("parameters"),
        )
        target_field = mapping["target_field"]
        output[target_field] = value
        context[target_field] = value

    # aggregations across all records in the group
    for agg in plan.get("aggregations", []):
        value = apply_aggregation_to_records(
            function=agg["function"],
            records=records,
            source_path=agg.get("source_path"),
            source_paths=agg.get("source_paths"),
        )
        target_field = agg["target_field"]
        output[target_field] = value
        context[target_field] = value

    # filters after mappings + aggregations
    filters = plan.get("filters", [])
    if filters and not record_passes_filters(context, filters):
        return None

    return output


def execute_plan_on_fixture(
    input_fixture: dict[str, Any],
    plan: dict[str, Any],
) -> dict[str, Any]:
    records = input_fixture.get("records", [])
    group_by_paths = plan.get("group_by_paths", [])

    # non-grouped mode
    if not group_by_paths:
        outputs = []
        for record in records:
            produced = execute_plan_on_record(record, plan)
            if produced is not None:
                outputs.append(produced)
        return {"records": outputs}

    # grouped mode
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for record in records:
        key = _build_group_key(record, group_by_paths)
        grouped.setdefault(key, []).append(record)

    outputs = []
    for _, group_records in grouped.items():
        produced = execute_plan_on_group(group_records, plan)
        if produced is not None:
            outputs.append(produced)

    return {"records": outputs}