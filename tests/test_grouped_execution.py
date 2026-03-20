from lsmbench.execution.engine import execute_plan_on_fixture


def test_grouped_execution_navigation_session_summary():
    input_fixture = {
        "records": [
            {
                "SESSION": "sess-1",
                "APP": "healthyw8.gamebus.eu",
                "FOR_CAMPAIGN": "HW8_YA_HB",
                "URI": "/account",
                "X_DATE": "2025-10-04 16:12:08",
            },
            {
                "SESSION": "sess-1",
                "APP": "healthyw8.gamebus.eu",
                "FOR_CAMPAIGN": "HW8_YA_HB",
                "URI": "/dashboard",
                "X_DATE": "2025-10-04 16:13:10",
            },
            {
                "SESSION": "sess-1",
                "APP": "healthyw8.gamebus.eu",
                "FOR_CAMPAIGN": "HW8_YA_HB",
                "URI": "/account",
                "X_DATE": "2025-10-04 16:14:00",
            },
            {
                "SESSION": "sess-2",
                "APP": "healthyw8.gamebus.eu",
                "FOR_CAMPAIGN": "HW8_YA_HB",
                "URI": "/home",
                "X_DATE": "2025-10-04 18:00:00",
            },
        ]
    }

    plan = {
        "plan_id": "TEST_GROUP_001",
        "task_id": "TEST_GROUP_001",
        "target_entity": "navigation_session_summary",
        "group_by_paths": ["$.SESSION"],
        "field_mappings": [
            {"target_field": "session_id", "operation": "copy", "source_paths": ["$.SESSION"]},
            {"target_field": "app", "operation": "copy", "source_paths": ["$.APP"]},
            {"target_field": "campaign", "operation": "copy", "source_paths": ["$.FOR_CAMPAIGN"]},
        ],
        "joins": [],
        "filters": [],
        "aggregations": [
            {"target_field": "page_visit_count", "function": "count", "source_path": "$.URI"},
            {"target_field": "first_uri", "function": "first", "source_path": "$.URI"},
            {"target_field": "last_uri", "function": "latest", "source_path": "$.URI"},
            {"target_field": "distinct_uri_count", "function": "distinct_count", "source_path": "$.URI"},
        ],
        "assumptions": [],
    }

    produced = execute_plan_on_fixture(input_fixture, plan)

    assert produced == {
        "records": [
            {
                "session_id": "sess-1",
                "app": "healthyw8.gamebus.eu",
                "campaign": "HW8_YA_HB",
                "page_visit_count": 3,
                "first_uri": "/account",
                "last_uri": "/account",
                "distinct_uri_count": 2,
            },
            {
                "session_id": "sess-2",
                "app": "healthyw8.gamebus.eu",
                "campaign": "HW8_YA_HB",
                "page_visit_count": 1,
                "first_uri": "/home",
                "last_uri": "/home",
                "distinct_uri_count": 1,
            },
        ]
    }