from __future__ import annotations

from typing import Any

from lsmbench.generation.type_normalization_variants import type_normalization_variants


def _obj_schema(fields: list[tuple[str, dict[str, Any]]], *, title: str) -> dict[str, Any]:
    return {
        "type": "object",
        "title": title,
        "properties": {name: spec for name, spec in fields},
        "required": [name for name, _ in fields],
        "additionalProperties": False,
    }


def _difficulty_from_operation(operation: str) -> str:
    if operation in {"cast_integer", "parse_date", "truncate_date"}:
        return "easy"
    if operation in {"normalize_boolean", "normalize_enum"}:
        return "medium"
    return "medium"


def _distractor_value(field_name: str) -> Any:
    if field_name == "created_at":
        return "2024-01-01T00:00:00Z"
    return "dummy"


def build_type_normalization_specs() -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []
    variants = type_normalization_variants()

    for offset, variant in enumerate(variants, start=201):
        task_id = f"SYN_{offset:03d}"

        fields: list[tuple[str, dict[str, Any]]] = [
            (variant.source_field, variant.source_type or {"type": "string"})
        ]
        input_record: dict[str, Any] = {
            variant.source_field: variant.source_value
        }

        for distractor_field in variant.distractor_fields:
            fields.append((distractor_field, {"type": "string"}))
            input_record[distractor_field] = _distractor_value(distractor_field)

        source_schema = _obj_schema(fields, title=f"{task_id}Source")
        target_schema = _obj_schema(
            [(variant.target_field, variant.target_type or {"type": "string"})],
            title=f"{task_id}Target",
        )

        specs.append(
            {
                "task_id": task_id,
                "title": f"Type normalization primitive: {variant.perturbation}",
                "split": "synthetic",
                "difficulty": _difficulty_from_operation(variant.operation),
                "source_family": "synthetic_type_normalization",
                "target_entity": variant.target_entity,
                                "task_text": (
                    f"Map source field '{variant.source_field}' to target field "
                    f"'{variant.target_field}' using the correct normalization operation."
                    + (
                        f" Use these operation parameters: {variant.parameters}."
                        if variant.parameters
                        else ""
                    )
                ),
                "primitive_family": "type_normalization",
                "primitive_subtype": variant.operation,
                "lexical_perturbation": "none",
                "ambiguity_class": "single_gold",
                "composition_depth": 1,
                "difficulty_axes": {
                    "num_source_objects": 1,
                    "num_target_fields": 1,
                    "num_joins": 0,
                    "num_aggregations": 0,
                    "nesting_depth": 0,
                    "has_enum_normalization": variant.operation == "normalize_enum",
                    "has_time_normalization": variant.operation in {"parse_date", "truncate_date"},
                },
                "source_schema": source_schema,
                "target_schema": target_schema,
                "input_fixture": {
                    "records": [input_record]
                },
                "expected_fixture": {
                    "records": [{variant.target_field: variant.expected_value}]
                },
                "matches": [
                    {
                        "target_field": variant.target_field,
                        "source_path": f"$.{variant.source_field}",
                        "relation": "semantic_match",
                    }
                ],
                "plan": {
                    "plan_id": f"{task_id}_gold",
                    "task_id": task_id,
                    "target_entity": variant.target_entity,
                    "field_mappings": [
                        {
                            "target_field": variant.target_field,
                            "operation": variant.operation,
                            "source_paths": [f"$.{variant.source_field}"],
                            "parameters": variant.parameters or {},
                        }
                    ],
                    "joins": [],
                    "filters": [],
                    "aggregations": [],
                    "assumptions": [variant.note] if variant.note else [],
                },
                "invariants": [
                    {
                        "type": "required_fields_present",
                        "fields": [variant.target_field],
                        "scope": "all_records",
                    }
                ],
                "tags": [
                    "synthetic",
                    "primitive",
                    "type_normalization",
                    variant.operation,
                    variant.perturbation,
                ],
                "notes": [variant.note] if variant.note else [],
                "downstream_checks": [
                    {
                        "type": "required_fields_present",
                        "fields": [variant.target_field],
                        "scope": "all_records",
                    }
                ],
            }
        )

    return specs