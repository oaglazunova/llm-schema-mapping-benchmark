from __future__ import annotations

from lsmbench.evaluation.plan_repair import repair_plan_paths


def test_repair_join_keys_and_join_type():
    plan = {
        "plan_id": "p1",
        "task_id": "SYN_301",
        "target_entity": "order_with_customer",
        "field_mappings": [
            {
                "target_field": "order_id",
                "operation": "copy",
                "source_paths": ["$.orders.id"],
            }
        ],
        "joins": [
            {
                "left_path": "$.orders",
                "right_path": "$.customers",
                "left_key": "$.orders.customer_id",
                "right_key": "$.id",
                "join_type": "LEFT",
            }
        ],
        "filters": [],
        "aggregations": [],
        "assumptions": [],
    }

    repaired, notes = repair_plan_paths(plan)

    assert repaired["joins"][0]["left_key"] == "customer_id"
    assert repaired["joins"][0]["right_key"] == "id"
    assert repaired["joins"][0]["join_type"] == "left"
    assert notes


def test_repair_assumptions_object_to_string():
    plan = {
        "plan_id": "p1",
        "task_id": "SYN_301",
        "target_entity": "order_with_customer",
        "field_mappings": [],
        "joins": [],
        "filters": [],
        "aggregations": [],
        "assumptions": [
            {
                "type": "boolean",
                "description": "Assume there are no duplicate orders.",
                "value": True,
            }
        ],
    }

    repaired, notes = repair_plan_paths(plan)

    assert repaired["assumptions"] == ["Assume there are no duplicate orders."]
    assert notes