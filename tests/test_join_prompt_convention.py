from __future__ import annotations

from lsmbench.evaluation.plan_repair import repair_plan_paths


def test_join_paths_should_not_embed_predicate_logic():
    plan = {
        "plan_id": "p1",
        "task_id": "SYN_301",
        "target_entity": "order_with_customer",
        "field_mappings": [
            {
                "target_field": "customer_email",
                "operation": "coalesce",
                "source_paths": [
                    "$.customers[?(@.id==$.orders.customer_id)].email",
                    "$.orders.customer_id",
                ],
            }
        ],
        "joins": [
            {
                "left_path": "$.orders",
                "right_path": "$.customers",
                "left_key": "customer_id",
                "right_key": "id",
                "join_type": "left",
            }
        ],
        "filters": [],
        "aggregations": [],
        "assumptions": [],
    }

    repaired, notes = repair_plan_paths(plan)

    # Repair should not try to "fix" semantic join expressions embedded in source_paths.
    assert repaired == plan
    assert notes == []