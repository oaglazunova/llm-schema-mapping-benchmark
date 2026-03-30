from __future__ import annotations

from typing import Any

from lsmbench.generation.lexical_perturbations import copy_rename_variants


def _obj_schema(fields: list[tuple[str, dict[str, Any]]], *, title: str) -> dict[str, Any]:
    return {
        "type": "object",
        "title": title,
        "properties": {name: spec for name, spec in fields},
        "required": [name for name, _ in fields],
        "additionalProperties": False,
    }


def build_copy_rename_specs() -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []

    variants = copy_rename_variants()

    base_examples = [
        {
            "task_id": "SYN_101",
            "difficulty": "easy",
            "target_entity": "canonical_user_profile",
            "target_field": "birth_date",
            "target_type": {"type": "string"},
            "example_value": "1992-03-14"
        },
        {
            "task_id": "SYN_102",
            "difficulty": "easy",
            "target_entity": "canonical_activity_summary",
            "target_field": "steps",
            "target_type": {"type": "integer"},
            "example_value": 7421
        },
    ]

    out_idx = 0
    for base in base_examples:
        for variant in variants:
            out_idx += 1
            task_id = f"SYN_{100 + out_idx:03d}"

            source_field = variant.source_label
            target_field = base["target_field"]

            source_schema = _obj_schema(
                [(source_field, {"type": "string" if target_field == "birth_date" else "integer"})],
                title=f"{task_id}Source",
            )
            target_schema = _obj_schema(
                [(target_field, base["target_type"])],
                title=f"{task_id}Target",
            )

            input_fixture = {
                "records": [
                    {source_field: base["example_value"]}
                ]
            }
            expected_fixture = {
                "records": [
                    {target_field: base["example_value"]}
                ]
            }

            specs.append(
                {
                    "task_id": task_id,
                    "title": f"Copy/rename primitive: {variant.perturbation}",
                    "split": "synthetic",
                    "difficulty": base["difficulty"],
                    "source_family": "synthetic_copy_rename",
                    "target_entity": base["target_entity"],
                    "task_text": (
                        f"Map source field '{source_field}' to target field '{target_field}'."
                    ),
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
                    "input_fixture": input_fixture,
                    "expected_fixture": expected_fixture,
                    "matches": [
                        {
                            "target_field": target_field,
                            "source_path": f"$.{source_field}",
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
                                "source_paths": [f"$.{source_field}"],
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