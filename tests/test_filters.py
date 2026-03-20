from lsmbench.execution.engine import execute_plan_on_fixture
from lsmbench.execution.filters import record_passes_filter


def test_record_passes_filter_eq():
    context = {"status": "finished"}
    filt = {"path": "$.status", "operator": "eq", "value": "finished"}
    assert record_passes_filter(context, filt) is True


def test_record_passes_filter_exists():
    context = {"uri": "/account"}
    filt = {"path": "$.uri", "operator": "exists", "value": None}
    assert record_passes_filter(context, filt) is True


def test_execute_plan_with_filter_on_source_field():
    input_fixture = {
        "records": [
            {"status": "finished", "id": "a1"},
            {"status": "cancelled", "id": "a2"},
        ]
    }

    plan = {
        "plan_id": "TEST_FILTER_001",
        "task_id": "TEST_FILTER_001",
        "target_entity": "mini_status_record",
        "field_mappings": [
            {"target_field": "source_id", "operation": "copy", "source_paths": ["$.id"]},
            {"target_field": "status", "operation": "copy", "source_paths": ["$.status"]},
        ],
        "joins": [],
        "filters": [
            {"path": "$.status", "operator": "eq", "value": "finished"}
        ],
        "aggregations": [],
        "assumptions": [],
    }

    produced = execute_plan_on_fixture(input_fixture, plan)

    assert produced == {
        "records": [
            {"source_id": "a1", "status": "finished"}
        ]
    }


def test_execute_plan_with_filter_on_derived_field():
    input_fixture = {
        "records": [
            {"value": "1", "id": "a1"},
            {"value": "3", "id": "a2"},
        ]
    }

    plan = {
        "plan_id": "TEST_FILTER_002",
        "task_id": "TEST_FILTER_002",
        "target_entity": "mini_value_record",
        "field_mappings": [
            {"target_field": "source_id", "operation": "copy", "source_paths": ["$.id"]},
            {"target_field": "value_num", "operation": "cast_integer", "source_paths": ["$.value"]},
        ],
        "joins": [],
        "filters": [
            {"path": "$.value_num", "operator": "gte", "value": 2}
        ],
        "aggregations": [],
        "assumptions": [],
    }

    produced = execute_plan_on_fixture(input_fixture, plan)

    assert produced == {
        "records": [
            {"source_id": "a2", "value_num": 3}
        ]
    }