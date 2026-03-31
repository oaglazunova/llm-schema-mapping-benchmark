from __future__ import annotations

from typing import Any, Callable

from lsmbench.generation.lexical_perturbations import (
    LexicalVariant,
    birth_date_variants,
    steps_variants,
)


def _obj_schema(fields: list[tuple[str, dict[str, Any]]], *, title: str) -> dict[str, Any]:
    return {
        "type": "object",
        "title": title,
        "properties": {name: spec for name, spec in fields},
        "required": [name for name, _ in fields],
        "additionalProperties": False,
    }


def _source_field_type_for_target(target_field: str) -> dict[str, Any]:
    if target_field == "birth_date":
        return {"type": "string"}
    if target_field == "steps":
        return {"type": "integer"}
    raise ValueError(f"Unsupported target_field={target_field!r}")


def _distractor_value_for_target(target_field: str) -> Any:
    if target_field == "birth_date":
        return "2024-01-01"
    if target_field == "steps":
        return 1200
    raise ValueError(f"Unsupported target_field={target_field!r}")


def build_copy_rename_specs() -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []

    base_examples: list[dict[str, Any]] = [
        {
            "task_id_start": 101,
            "difficulty": "easy",
            "target_entity": "canonical_user_profile",
            "target_field": "birth_date",
            "target_type": {"type": "string"},
            "example_value": "1992-03-14",
            "variants_fn": birth_date_variants,
        },
        {
            "task_id_start": 107,
            "difficulty": "easy",
            "target_entity": "canonical_activity_summary",
            "target_field": "steps",
            "target_type": {"type": "integer"},
            "example_value": 7421,
            "variants_fn": steps_variants,
        },
    ]

    for base in base_examples:
        target_field = base["target_field"]
        source_type = _source_field_type_for_target(target_field)
        variants: list[LexicalVariant] = base["variants_fn"]()

        for offset, variant in enumerate(variants):
            task_id = f"SYN_{base['task_id_start'] + offset:03d}"

            fields: list[tuple[str, dict[str, Any]]] = [
                (variant.source_field, source_type),
            ]
            input_record: dict[str, Any] = {
                variant.source_field: base["example_value"]
            }

            for distractor_field in variant.distractor_fields:
                fields.append((distractor_field, source_type))
                input_record[distractor_field] = _distractor_value_for_target(target_field)

            source_schema = _obj_schema(fields, title=f"{task_id}Source")
            target_schema = _obj_schema(
                [(target_field, base["target_type"])],
                title=f"{task_id}Target",
            )

            task_text = f"Map source field '{variant.source_field}' to target field '{target_field}'."
            if variant.distractor_fields:
                task_text += " Ignore distractor fields with similar type or meaning."

            specs.append(
                {
                    "task_id": task_id,
                    "title": f"Copy/rename primitive: {target_field} / {variant.perturbation}",
                    "split": "synthetic",
                    "difficulty": base["difficulty"],
                    "source_family": "synthetic_copy_rename",
                    "target_entity": base["target_entity"],
                    "task_text": task_text,
                    "primitive_family": "copy_rename",
                    "primitive_subtype": variant.perturbation,
                    "lexical_perturbation": variant.perturbation,
                    "ambiguity_class": "single_gold",
                    "composition_depth": 1,
                    "difficulty_axes": {
                        "num_source_objects": 1,
                        "num_target_fields": 1,
                        "num_joins": 0,
                        "num_aggregations": 0,
                        "nesting_depth": 0,
                        "has_enum_normalization": False,
                        "has_time_normalization": False,
                    },
                    "source_schema": source_schema,
                    "target_schema": target_schema,
                    "input_fixture": {
                        "records": [input_record]
                    },
                    "expected_fixture": {
                        "records": [{target_field: base["example_value"]}]
                    },
                    "matches": [
                        {
                            "target_field": target_field,
                            "source_path": f"$.{variant.source_field}",
                            "relation": variant.perturbation,
                        }
                    ],
                    "plan": {
                        "plan_id": f"{task_id}_gold",
                        "task_id": task_id,
                        "target_entity": base["target_entity"],
                        "field_mappings": [
                            {
                                "target_field": target_field,
                                "operation": "copy",
                                "source_paths": [f"$.{variant.source_field}"],
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
                            "fields": [target_field],
                            "scope": "all_records",
                        }
                    ],
                    "tags": [
                        "synthetic",
                        "primitive",
                        "copy_rename",
                        target_field,
                        variant.perturbation,
                    ],
                    "notes": [variant.note] if variant.note else [],
                    "downstream_checks": [
                        {
                            "type": "required_fields_present",
                            "fields": [target_field],
                            "scope": "all_records",
                        }
                    ],
                }
            )

    return specs