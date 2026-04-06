from __future__ import annotations

from typing import Any


def _contains_embedded_join_logic(path: str) -> bool:
    """
    Reject predicate/filter-style expressions inside source_paths for this benchmark IR.
    The join must be expressed in `joins`, not inside field-mapping paths.
    """
    if not isinstance(path, str):
        return False

    # Examples to reject:
    # $.customers[?(@.id==$.orders.customer_id)].email
    # $.customers[id == $.orders.customer_id].email
    return any(token in path for token in ["[?", "==", "[id ", "[@."])


def _root_token(path: str) -> str | None:
    if not isinstance(path, str):
        return None
    if not path.startswith("$."):
        return None

    stripped = path[2:]
    if not stripped:
        return None

    root = stripped.split(".", 1)[0]
    root = root.split("[", 1)[0]
    return root or None


def _source_field_names(source_schema: dict[str, Any]) -> set[str]:
    if "fields" in source_schema and isinstance(source_schema["fields"], list):
        return {
            field["name"]
            for field in source_schema["fields"]
            if isinstance(field, dict) and "name" in field
        }

    props = source_schema.get("properties", {})
    if isinstance(props, dict):
        return set(props.keys())

    return set()


def _target_field_names(target_schema: dict[str, Any]) -> set[str]:
    if "fields" in target_schema and isinstance(target_schema["fields"], list):
        return {
            field["name"]
            for field in target_schema["fields"]
            if isinstance(field, dict) and "name" in field
        }

    props = target_schema.get("properties", {})
    if isinstance(props, dict):
        return set(props.keys())

    return set()


def validate_references(task: dict[str, Any], plan: dict[str, Any]) -> dict[str, Any]:
    errors: list[str] = []

    source_fields = _source_field_names(task["source_schema"])
    target_fields = _target_field_names(task["target_schema"])

    for i, fm in enumerate(plan.get("field_mappings", [])):
        target_field = fm["target_field"]
        if target_field not in target_fields:
            errors.append(
                f"field_mappings[{i}]: target_field {target_field!r} does not exist in target_schema"
            )

        for j, path in enumerate(fm.get("source_paths", [])):
            root = _root_token(path)
            if root is None:
                errors.append(
                    f"field_mappings[{i}].source_paths[{j}] {path!r}: invalid path format"
                )
                continue

            if _contains_embedded_join_logic(path):
                errors.append(
                    f"field_mappings[{i}].source_paths[{j}] {path!r}: "
                    "join logic must be declared in joins, not embedded in source_paths"
                )
                continue

            if root not in source_fields and root not in target_fields:
                errors.append(
                    f"field_mappings[{i}].source_paths[{j}] {path!r}: root field {root!r} does not exist in source or target schema"
                )

    for i, flt in enumerate(plan.get("filters", [])):
        path = flt["path"]
        root = _root_token(path)
        if root is None:
            errors.append(f"filters[{i}].path {path!r}: invalid path format")
        elif root not in source_fields and root not in target_fields:
            errors.append(
                f"filters[{i}].path {path!r}: root field {root!r} does not exist in source or target schema"
            )

    for i, agg in enumerate(plan.get("aggregations", [])):
        target_field = agg["target_field"]
        if target_field not in target_fields:
            errors.append(
                f"aggregations[{i}]: target_field {target_field!r} does not exist in target_schema"
            )

        if "source_path" in agg:
            root = _root_token(agg["source_path"])
            if root is None:
                errors.append(
                    f"aggregations[{i}].source_path {agg['source_path']!r}: invalid path format"
                )
            elif root not in source_fields and root not in target_fields:
                errors.append(
                    f"aggregations[{i}].source_path {agg['source_path']!r}: root field {root!r} does not exist in source or target schema"
                )

        for j, path in enumerate(agg.get("source_paths", [])):
            root = _root_token(path)
            if root is None:
                errors.append(
                    f"aggregations[{i}].source_paths[{j}] {path!r}: invalid path format"
                )
            elif root not in source_fields and root not in target_fields:
                errors.append(
                    f"aggregations[{i}].source_paths[{j}] {path!r}: root field {root!r} does not exist in source or target schema"
                )

    return {
        "valid": len(errors) == 0,
        "errors": errors,
    }