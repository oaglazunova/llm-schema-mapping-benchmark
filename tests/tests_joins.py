from lsmbench.execution.engine import execute_plan_on_fixture
from lsmbench.execution.joins import materialize_single_join


def test_materialize_single_join_left():
    bundle = {
        "orders": [
            {"id": "o1", "customer_id": "c1", "amount": 12.5},
            {"id": "o2", "customer_id": "c2", "amount": 7.0},
            {"id": "o3", "customer_id": "c9", "amount": 3.0},
        ],
        "customers": [
            {"id": "c1", "email": "a@example.org"},
            {"id": "c2", "email": "b@example.org"},
        ],
    }

    join_spec = {
        "left_path": "$.orders",
        "right_path": "$.customers",
        "left_key": "customer_id",
        "right_key": "id",
        "join_type": "left",
    }

    rows = materialize_single_join(bundle, join_spec)

    assert rows == [
        {
            "orders": {"id": "o1", "customer_id": "c1", "amount": 12.5},
            "customers": {"id": "c1", "email": "a@example.org"},
        },
        {
            "orders": {"id": "o2", "customer_id": "c2", "amount": 7.0},
            "customers": {"id": "c2", "email": "b@example.org"},
        },
        {
            "orders": {"id": "o3", "customer_id": "c9", "amount": 3.0},
            "customers": None,
        },
    ]


def test_execute_plan_with_join():
    input_fixture = {
        "records": [
            {
                "orders": [
                    {"id": "o1", "customer_id": "c1", "amount": 12.5},
                    {"id": "o2", "customer_id": "c2", "amount": 7.0},
                ],
                "customers": [
                    {"id": "c1", "email": "a@example.org"},
                    {"id": "c2", "email": "b@example.org"},
                ],
            }
        ]
    }

    plan = {
        "plan_id": "TEST_JOIN_001",
        "task_id": "TEST_JOIN_001",
        "target_entity": "order_with_customer",
        "field_mappings": [
            {"target_field": "order_id", "operation": "copy", "source_paths": ["$.orders.id"]},
            {"target_field": "amount", "operation": "copy", "source_paths": ["$.orders.amount"]},
            {"target_field": "customer_email", "operation": "copy", "source_paths": ["$.customers.email"]},
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

    produced = execute_plan_on_fixture(input_fixture, plan)

    assert produced == {
        "records": [
            {
                "order_id": "o1",
                "amount": 12.5,
                "customer_email": "a@example.org",
            },
            {
                "order_id": "o2",
                "amount": 7.0,
                "customer_email": "b@example.org",
            },
        ]
    }