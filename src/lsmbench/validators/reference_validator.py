from __future__ import annotations

import re
from typing import Any


_JSONPATH_TOKEN_RE = re.compile(
    r"""
    (?:
        \.([A-Za-z_][A-Za-z0-9_]*)
    )
    |
    (?:
        \[(\*|\d+)\]
    )
    """,
    re.VERBOSE,
)


def _tokenize_json_path(path: str) -> list[tuple[str, str]]:
    """
    Supports a minimal JSONPath subset:
    - $.field
    - $.field.subfield
    - $.array[0]
    - $.array[*]
    - $.array[*].field
    """
    if not path.startswith("$"):
        raise ValueError(f"Path must start with '$': {path}")

    tokens: list[tuple[str, str]] = []
    for match in _JSONPATH_TOKEN_RE.finditer(path[1:]):
        prop, idx = match.groups()
        if prop is not None:
            tokens.append(("prop", prop))
        elif idx is not None:
            tokens.append(("index", idx))
    return tokens


def _schema_type(schema: dict[str, Any]) -> str | None:
    t = schema.get("type")
    return t if isinstance(t, str) else None


def _resolve_path_against_schema(schema: dict[str, Any], path: str) -> tuple[bool, str | None]:
    """
    Resolve a path against a JSON Schema-like object schema.
    """
    try:
        tokens = _tokenize_json_path(path)
    except ValueError as e:
        return False, str(e)

    current = schema

    for kind, value in tokens:
        stype = _schema_type(current)

        if kind == "prop":
            if stype != "object":
                return False, f"Expected object before property '{value}' in path '{path}', got '{stype}'"

            props = current.get("properties", {})
            if value not in props:
                return False, f"Property '{value}' not found in path '{path}'"

            current = props[value]

        elif kind == "index":
            if stype != "array":
                return False, f"Expected array before index '[{value}]' in path '{path}', got '{stype}'"

            items = current.get("items")
            if not isinstance(items, dict):
                return False, f"Array items schema missing for path '{path}'"

            current = items

    return True, None


def _get_schema_at_path(schema: dict[str, Any], path: str) -> tuple[dict[str, Any] | None, str | None]:
    """
    Resolve a path against a schema and return the schema object at that path.
    """
    try:
        tokens = _tokenize_json_path(path)
    except ValueError as e:
        return None, str(e)

    current = schema

    for kind, value in tokens:
        stype = _schema_type(current)

        if kind == "prop":
            if stype != "object":
                return None, f"Expected object before property '{value}' in path '{path}', got '{stype}'"

            props = current.get("properties", {})
            if value not in props:
                return None, f"Property '{value}' not found in path '{path}'"

            next_schema = props[value]
            if not isinstance(next_schema, dict):
                return None, f"Invalid schema node at property '{value}' in path '{path}'"
            current = next_schema

        elif kind == "index":
            if stype != "array":
                return None, f"Expected array before index '[{value}]' in path '{path}', got '{stype}'"

            items = current.get("items")
            if not isinstance(items, dict):
                return None, f"Array items schema missing for path '{path}'"

            current = items

    return current, None


def _target_field_exists(target_schema: dict[str, Any], field: str) -> bool:
    if target_schema.get("type") != "object":
        return False
    return field in target_schema.get("properties", {})


def _get_target_field_schema(target_schema: dict[str, Any], field: str) -> dict[str, Any] | None:
    if target_schema.get("type") != "object":
        return None
    props = target_schema.get("properties", {})
    schema = props.get(field)
    return schema if isinstance(schema, dict) else None


def _build_derived_root_schema(task: dict[str, Any], plan: dict[str, Any]) -> dict[str, Any]:
    """
    Build a minimal object schema representing fields created by field_mappings.

    Example:
    if a field_mapping writes target_field='consent_items', and target_schema defines
    consent_items as an array of objects, then paths like:
      $.consent_items[*].accepted
    should be considered valid as intermediate derived paths.
    """
    target_schema = task["target_schema"]
    derived_props: dict[str, Any] = {}

    for mapping in plan.get("field_mappings", []):
        target_field = mapping["target_field"]
        field_schema = _get_target_field_schema(target_schema, target_field)
        if field_schema is not None:
            derived_props[target_field] = field_schema

    return {
        "type": "object",
        "properties": derived_props,
        "additionalProperties": False,
    }


def _alias_from_path(path: str) -> str:
    tokens = _tokenize_json_path(path)
    prop_tokens = [value for kind, value in tokens if kind == "prop"]
    if not prop_tokens:
        raise ValueError(f"Could not derive alias from path: {path}")
    return prop_tokens[-1]


def _build_join_row_schema(source_schema: dict[str, Any], plan: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    """
    Build a schema representing the per-row execution context produced by joins.

    For a join:
      left_path = $.orders
      right_path = $.customers

    we build:

      {
        "type": "object",
        "properties": {
          "orders": <items schema of orders>,
          "customers": <items schema of customers>
        }
      }

    This matches how the execution engine materializes joined rows:
      {"orders": {...}, "customers": {...}}
    """
    join_props: dict[str, Any] = {}
    errors: list[str] = []

    for i, join in enumerate(plan.get("joins", [])):
        left_path = join["left_path"]
        right_path = join["right_path"]

        left_schema, left_err = _get_schema_at_path(source_schema, left_path)
        if left_schema is None:
            errors.append(f"joins[{i}].left_path '{left_path}': {left_err}")
            continue

        right_schema, right_err = _get_schema_at_path(source_schema, right_path)
        if right_schema is None:
            errors.append(f"joins[{i}].right_path '{right_path}': {right_err}")
            continue

        if _schema_type(left_schema) != "array":
            errors.append(f"joins[{i}].left_path '{left_path}' must resolve to an array")
            continue

        if _schema_type(right_schema) != "array":
            errors.append(f"joins[{i}].right_path '{right_path}' must resolve to an array")
            continue

        left_items = left_schema.get("items")
        right_items = right_schema.get("items")

        if not isinstance(left_items, dict):
            errors.append(f"joins[{i}].left_path '{left_path}' has no valid items schema")
            continue

        if not isinstance(right_items, dict):
            errors.append(f"joins[{i}].right_path '{right_path}' has no valid items schema")
            continue

        left_alias = _alias_from_path(left_path)
        right_alias = _alias_from_path(right_path)

        join_props[left_alias] = left_items
        join_props[right_alias] = right_items

    return {
        "type": "object",
        "properties": join_props,
        "additionalProperties": False,
    }, errors


def _resolve_path_against_source_or_derived(
    source_schema: dict[str, Any],
    derived_schema: dict[str, Any],
    path: str,
) -> tuple[bool, str | None]:
    """
    Accept a path if it resolves against:
    - the original source schema, OR
    - the derived intermediate schema created by field_mappings
    """
    ok, msg = _resolve_path_against_schema(source_schema, path)
    if ok:
        return True, None

    ok2, msg2 = _resolve_path_against_schema(derived_schema, path)
    if ok2:
        return True, None

    return False, f"{msg}; also not valid as derived path: {msg2}"


def _resolve_path_against_join_or_derived(
    join_row_schema: dict[str, Any],
    derived_schema: dict[str, Any],
    path: str,
) -> tuple[bool, str | None]:
    """
    Accept a path if it resolves against:
    - the joined-row schema, OR
    - the derived intermediate schema created by field_mappings
    """
    ok, msg = _resolve_path_against_schema(join_row_schema, path)
    if ok:
        return True, None

    ok2, msg2 = _resolve_path_against_schema(derived_schema, path)
    if ok2:
        return True, None

    return False, f"{msg}; also not valid as derived path: {msg2}"


def validate_references(task: dict[str, Any], plan: dict[str, Any]) -> dict[str, Any]:
    errors: list[str] = []

    source_schema = task["source_schema"]
    target_schema = task["target_schema"]
    derived_schema = _build_derived_root_schema(task, plan)

    joins = plan.get("joins", [])
    join_row_schema: dict[str, Any] | None = None

    if joins:
        join_row_schema, join_schema_errors = _build_join_row_schema(source_schema, plan)
        errors.extend(join_schema_errors)

    # field mappings:
    # - if no joins: source_paths must refer to original source_schema only
    # - if joins exist: source_paths must refer to joined-row schema only
    for i, mapping in enumerate(plan.get("field_mappings", [])):
        target_field = mapping["target_field"]
        if not _target_field_exists(target_schema, target_field):
            errors.append(
                f"field_mappings[{i}]: target_field '{target_field}' does not exist in target_schema"
            )

        for j, source_path in enumerate(mapping.get("source_paths", [])):
            if joins:
                ok, msg = _resolve_path_against_schema(join_row_schema, source_path)
            else:
                ok, msg = _resolve_path_against_schema(source_schema, source_path)

            if not ok:
                errors.append(
                    f"field_mappings[{i}].source_paths[{j}] '{source_path}': {msg}"
                )

    # joins:
    # join paths must refer to original source_schema only
    for i, join in enumerate(joins):
        for key in ("left_path", "right_path"):
            path = join[key]
            ok, msg = _resolve_path_against_schema(source_schema, path)
            if not ok:
                errors.append(f"joins[{i}].{key} '{path}': {msg}")

    # group_by_paths:
    # grouping paths must refer to the original source schema only
    for i, group_path in enumerate(plan.get("group_by_paths", [])):
        ok, msg = _resolve_path_against_schema(source_schema, group_path)
        if not ok:
            errors.append(f"group_by_paths[{i}] '{group_path}': {msg}")

    # filters:
    # - if no joins: filters may refer to source or derived paths
    # - if joins exist: filters may refer to joined-row or derived paths
    for i, filt in enumerate(plan.get("filters", [])):
        path = filt["path"]
        if joins:
            ok, msg = _resolve_path_against_join_or_derived(join_row_schema, derived_schema, path)
        else:
            ok, msg = _resolve_path_against_source_or_derived(source_schema, derived_schema, path)

        if not ok:
            errors.append(f"filters[{i}].path '{path}': {msg}")

    # aggregations:
    # - if no joins: aggregations may refer to source or derived paths
    # - if joins exist: aggregations may refer to joined-row or derived paths
    for i, agg in enumerate(plan.get("aggregations", [])):
        target_field = agg["target_field"]
        if not _target_field_exists(target_schema, target_field):
            errors.append(
                f"aggregations[{i}]: target_field '{target_field}' does not exist in target_schema"
            )

        if "source_path" in agg:
            source_path = agg["source_path"]
            if joins:
                ok, msg = _resolve_path_against_join_or_derived(join_row_schema, derived_schema, source_path)
            else:
                ok, msg = _resolve_path_against_source_or_derived(source_schema, derived_schema, source_path)

            if not ok:
                errors.append(f"aggregations[{i}].source_path '{source_path}': {msg}")

        for j, source_path in enumerate(agg.get("source_paths", [])):
            if joins:
                ok, msg = _resolve_path_against_join_or_derived(join_row_schema, derived_schema, source_path)
            else:
                ok, msg = _resolve_path_against_source_or_derived(source_schema, derived_schema, source_path)

            if not ok:
                errors.append(
                    f"aggregations[{i}].source_paths[{j}] '{source_path}': {msg}"
                )

    return {
        "valid": len(errors) == 0,
        "errors": errors,
    }