from lsmbench.benchmark.task_builder import (
    build_empty_plan,
    build_task_skeleton,
    infer_schema_from_example,
)


def test_infer_schema_from_example_object():
    example = {
        "id": "abc",
        "amount": 12.5,
        "customer": {
            "email": "a@example.org",
            "vip": True,
        },
    }

    schema = infer_schema_from_example(example, title="Example")

    assert schema["type"] == "object"
    assert schema["title"] == "Example"
    assert "id" in schema["properties"]
    assert schema["properties"]["id"]["type"] == "string"
    assert schema["properties"]["amount"]["type"] == "number"
    assert schema["properties"]["customer"]["type"] == "object"


def test_build_task_skeleton():
    source_schema = {
        "type": "object",
        "properties": {"id": {"type": "string"}},
        "required": ["id"],
        "additionalProperties": False,
    }
    target_schema = {
        "type": "object",
        "properties": {"target_id": {"type": "string"}},
        "required": ["target_id"],
        "additionalProperties": False,
    }

    task = build_task_skeleton(
        task_id="PUB_999",
        title="Example task",
        split="public",
        difficulty="easy",
        source_family="example_source",
        target_entity="example_target",
        task_text="Example task text",
        source_schema=source_schema,
        target_schema=target_schema,
        input_fixture_ref="benchmark/fixtures/public/PUB_999_input.json",
        expected_fixture_ref="benchmark/fixtures/public/PUB_999_expected.json",
        tags=["example"],
        notes=["test"],
    )

    assert task["task_id"] == "PUB_999"
    assert task["split"] == "public"
    assert task["gold_refs"]["plan"] == "benchmark/gold/plans/PUB_999_plan.json"


def test_build_empty_plan():
    plan = build_empty_plan(task_id="PUB_999", target_entity="example_target")

    assert plan["plan_id"] == "PUB_999_gold"
    assert plan["task_id"] == "PUB_999"
    assert plan["target_entity"] == "example_target"
    assert plan["field_mappings"] == []