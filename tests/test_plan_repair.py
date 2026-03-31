from __future__ import annotations

from lsmbench.evaluation.plan_repair import repair_plan_paths


def test_repair_plan_paths_fixes_simple_source_path_spacing():
    plan = {
        "plan_id": "p1",
        "task_id": "SYN_201",
        "target_entity": "canonical_user_profile",
        "field_mappings": [
            {
                "target_field": "birth_date",
                "operation": "parse_date",
                "source_paths": ["$ dob_text"],
            }
        ],
        "joins": [],
        "filters": [],
        "aggregations": [],
        "assumptions": [],
    }

    repaired, notes = repair_plan_paths(plan)

    assert repaired["field_mappings"][0]["source_paths"] == ["$.dob_text"]
    assert notes


def test_repair_plan_paths_fixes_missing_dot():
    plan = {
        "plan_id": "p1",
        "task_id": "SYN_106",
        "target_entity": "canonical_user_profile",
        "field_mappings": [
            {
                "target_field": "birth_date",
                "operation": "copy",
                "source_paths": ["$participant_id"],
            }
        ],
        "joins": [],
        "filters": [],
        "aggregations": [],
        "assumptions": [],
    }

    repaired, notes = repair_plan_paths(plan)

    assert repaired["field_mappings"][0]["source_paths"] == ["$.participant_id"]
    assert notes


def test_repair_plan_paths_does_not_change_valid_path():
    plan = {
        "plan_id": "p1",
        "task_id": "SYN_202",
        "target_entity": "canonical_user_profile",
        "field_mappings": [
            {
                "target_field": "birth_date",
                "operation": "parse_date",
                "source_paths": ["$.fecha_nacimiento"],
            }
        ],
        "joins": [],
        "filters": [],
        "aggregations": [],
        "assumptions": [],
    }

    repaired, notes = repair_plan_paths(plan)

    assert repaired == plan
    assert notes == []