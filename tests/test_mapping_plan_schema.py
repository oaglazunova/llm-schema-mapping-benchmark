from __future__ import annotations

from lsmbench.validators.schema_validator import validate_plan


def test_mapping_plan_schema_accepts_plain_target_field_names() -> None:
    plan = {
        "plan_id": "p1",
        "task_id": "SYN_101",
        "target_entity": "canonical_user_profile",
        "field_mappings": [
            {
                "target_field": "birth_date",
                "operation": "copy",
                "source_paths": ["$.birth_date"],
            }
        ],
        "joins": [],
        "filters": [],
        "aggregations": [],
        "assumptions": [],
    }

    report = validate_plan(plan)
    assert report["valid"], report["errors"]


def test_mapping_plan_schema_rejects_jsonpath_target_fields() -> None:
    plan = {
        "plan_id": "p1",
        "task_id": "SYN_101",
        "target_entity": "canonical_user_profile",
        "field_mappings": [
            {
                "target_field": "$.birth_date",
                "operation": "copy",
                "source_paths": ["$.birth_date"],
            }
        ],
        "joins": [],
        "filters": [],
        "aggregations": [],
        "assumptions": [],
    }

    report = validate_plan(plan)
    assert not report["valid"]