from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from lsmbench.execution.aggregations import apply_aggregation, apply_aggregation_to_records
from lsmbench.execution.filters import record_passes_filters
from lsmbench.execution.operations import apply_operation
from lsmbench.execution.json_path import resolve_one


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


def _validate_output_record(
    output_record: dict[str, Any],
    target_schema: dict[str, Any],
    record_index: int,
) -> list[ExecutionIssue]:
    """
    Validate output against the benchmark's target_schema format:

        {
          "target_entity": "...",
          "fields": [
            {"name": "...", "type": "...", "required": true},
            ...
          ]
        }
    """
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


def _get_source_value(record: dict[str, Any], source_path: str) -> Any:
    """
    Support:
      $.FIELD
      $.FIELD.subfield
    """
    if not source_path.startswith("$."):
        raise ValueError(f"Unsupported source path: {source_path}")

    parts = source_path[2:].split(".")
    cur: Any = record

    for part in parts:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)

    return cur


def _apply_field_mappings_to_record(
    record: dict[str, Any],
    plan_payload: dict[str, Any],
) -> dict[str, Any]:
    output_obj: dict[str, Any] = {}

    for mapping in plan_payload.get("field_mappings", []):
        target_field = mapping["target_field"]
        operation = mapping["operation"]
        source_paths = mapping.get("source_paths", [])
        parameters = mapping.get("parameters", {})

        value = apply_operation(
            operation=operation,
            context=record,
            source_paths=source_paths,
            parameters=parameters,
        )
        output_obj[target_field] = value

    return output_obj


def _apply_record_level_aggregations(
    output_obj: dict[str, Any],
    plan_payload: dict[str, Any],
) -> None:
    """
    Aggregations over already-produced fields inside a single output object.
    Example:
      $.consent_items[*].accepted
    """
    for agg in plan_payload.get("aggregations", []):
        target_field = agg["target_field"]
        function = agg["function"]

        if "source_path" in agg:
            value = apply_aggregation(
                function=function,
                context=output_obj,
                source_path=agg["source_path"],
            )
        else:
            value = apply_aggregation(
                function=function,
                context=output_obj,
                source_paths=agg.get("source_paths", []),
            )

        output_obj[target_field] = value


def _group_records(
    records: list[dict[str, Any]],
    group_by_paths: list[str],
) -> dict[tuple[Any, ...], list[dict[str, Any]]]:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for record in records:
        key = tuple(resolve_one(record, path) for path in group_by_paths)
        groups.setdefault(key, []).append(record)
    return groups


def _expand_joined_records(
    records: list[dict[str, Any]],
    joins: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Minimal v1 join support for bundle-shaped inputs like PUB_106.

    Assumes:
      - left_path points to an array in the bundle, e.g. $.orders
      - right_path points to an array in the bundle, e.g. $.customers
      - output should contain one record per left-side row
      - join_type supports 'left' and 'inner'
    """
    if not joins:
        return records

    # v1: support one join only
    join = joins[0]

    left_root = join["left_path"][2:]   # "$.orders" -> "orders"
    right_root = join["right_path"][2:] # "$.customers" -> "customers"
    left_key = join["left_key"]
    right_key = join["right_key"]
    join_type = join.get("join_type", "left")

    expanded: list[dict[str, Any]] = []

    for bundle in records:
        left_rows = bundle.get(left_root, [])
        right_rows = bundle.get(right_root, [])

        if not isinstance(left_rows, list):
            continue
        if not isinstance(right_rows, list):
            right_rows = []

        right_index = {}
        for row in right_rows:
            if isinstance(row, dict):
                right_index[row.get(right_key)] = row

        for left_row in left_rows:
            if not isinstance(left_row, dict):
                continue

            match = right_index.get(left_row.get(left_key))

            if match is None and join_type == "inner":
                continue

            expanded.append({
                left_root: left_row,
                right_root: match or {},
            })

    return expanded


def _execute_non_grouped(
    task_payload: dict[str, Any],
    plan_payload: dict[str, Any],
    records: list[dict[str, Any]],
) -> ExecutionResult:
    outputs: list[dict[str, Any]] = []
    issues: list[ExecutionIssue] = []

    filters = plan_payload.get("filters", [])

    for idx, record in enumerate(records):
        try:
            # Build initial output from source record
            output_obj = _apply_field_mappings_to_record(record, plan_payload)

            # Apply filters against source + derived fields
            filter_context = {}
            filter_context.update(record)
            filter_context.update(output_obj)
            if filters and not record_passes_filters(filter_context, filters):
                continue

            # Apply record-level aggregations
            _apply_record_level_aggregations(output_obj, plan_payload)

            outputs.append(output_obj)
            issues.extend(_validate_output_record(output_obj, task_payload["target_schema"], len(outputs) - 1))

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


def _execute_grouped(
    task_payload: dict[str, Any],
    plan_payload: dict[str, Any],
    records: list[dict[str, Any]],
) -> ExecutionResult:
    outputs: list[dict[str, Any]] = []
    issues: list[ExecutionIssue] = []

    group_by_paths = plan_payload.get("group_by_paths", [])
    if not group_by_paths:
        raise ValueError("Grouped execution requested but no group_by_paths provided")

    groups = _group_records(records, group_by_paths)

    for out_idx, (_, group_records) in enumerate(groups.items()):
        try:
            first_record = group_records[0]

            # Field mappings are taken from the first record in the group
            output_obj = _apply_field_mappings_to_record(first_record, plan_payload)

            # Grouped aggregations run over the entire group
            for agg in plan_payload.get("aggregations", []):
                target_field = agg["target_field"]
                function = agg["function"]

                if "source_path" in agg:
                    value = apply_aggregation_to_records(
                        function=function,
                        records=group_records,
                        source_path=agg["source_path"],
                    )
                else:
                    value = apply_aggregation_to_records(
                        function=function,
                        records=group_records,
                        source_paths=agg.get("source_paths", []),
                    )

                output_obj[target_field] = value

            outputs.append(output_obj)
            issues.extend(_validate_output_record(output_obj, task_payload["target_schema"], out_idx))

        except Exception as e:
            issues.append(
                ExecutionIssue(
                    level="error",
                    code="execution_error",
                    message=str(e),
                    record_index=out_idx,
                )
            )

    ok = not any(issue.level == "error" for issue in issues)
    return ExecutionResult(ok=ok, outputs=outputs, issues=issues)


def _fixture_to_records(input_fixture):
    if isinstance(input_fixture, list):
        return input_fixture

    if isinstance(input_fixture, dict):
        if "records" in input_fixture:
            return input_fixture["records"]
        return [input_fixture]

    raise ValueError(f"Unsupported fixture type: {type(input_fixture)!r}")


def execute_plan_on_records(
    task_payload: dict[str, Any],
    plan_payload: dict[str, Any],
    records: list[dict[str, Any]],
) -> ExecutionResult:

    records = _expand_joined_records(records, plan_payload.get("joins", []))

    if plan_payload.get("group_by_paths"):
        return _execute_grouped(task_payload, plan_payload, records)
    return _execute_non_grouped(task_payload, plan_payload, records)


def execute_plan_on_fixture(
    input_fixture: dict[str, Any],
    plan_payload: dict[str, Any],
    *,
    task_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Convenience wrapper used by tests.

    If task_payload is omitted, no target-schema validation is performed here.
    """
    records = _fixture_to_records(input_fixture)
    if not isinstance(records, list):
        raise ValueError("Fixture input must contain a list under 'records'")

    if task_payload is None:
        # Minimal execution path for tests that only care about produced records
        if plan_payload.get("group_by_paths"):
            groups = _group_records(records, plan_payload["group_by_paths"])
            out_records: list[dict[str, Any]] = []
            for _, group_records in groups.items():
                first_record = group_records[0]
                output_obj = _apply_field_mappings_to_record(first_record, plan_payload)
                for agg in plan_payload.get("aggregations", []):
                    if "source_path" in agg:
                        output_obj[agg["target_field"]] = apply_aggregation_to_records(
                            agg["function"], group_records, source_path=agg["source_path"]
                        )
                    else:
                        output_obj[agg["target_field"]] = apply_aggregation_to_records(
                            agg["function"], group_records, source_paths=agg.get("source_paths", [])
                        )
                out_records.append(output_obj)
            return {"records": out_records}

        out_records: list[dict[str, Any]] = []
        filters = plan_payload.get("filters", [])
        for record in records:
            output_obj = _apply_field_mappings_to_record(record, plan_payload)
            filter_context = {}
            filter_context.update(record)
            filter_context.update(output_obj)
            if filters and not record_passes_filters(filter_context, filters):
                continue
            _apply_record_level_aggregations(output_obj, plan_payload)
            out_records.append(output_obj)
        return {"records": out_records}

    result = execute_plan_on_records(task_payload, plan_payload, records)
    return {"records": result.outputs}


def execute_plan_from_files(
    task_path: str | Path,
    plan_path: str | Path,
    fixture_input_path: str | Path,
) -> ExecutionResult:
    task_payload = _load_json(task_path)
    plan_payload = _load_json(plan_path)
    fixture_input = _load_json(fixture_input_path)

    records = _fixture_to_records(fixture_input)

    return execute_plan_on_records(task_payload, plan_payload, records)