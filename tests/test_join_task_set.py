from __future__ import annotations

from lsmbench.benchmark.task_loader import load_tasks_from_task_set, load_gold
from lsmbench.validators.execution_validator import validate_execution


def test_synthetic_join_task_set_loads() -> None:
    tasks = load_tasks_from_task_set("benchmark/task_sets/synthetic_join_v1.json")
    assert tasks

    for task in tasks:
        assert task["primitive_family"] == "join"
        assert task["composition_depth"] == 1
        assert task["split"] == "synthetic"
        assert task["source_family"].startswith("synthetic_join_")


def test_join_tasks_have_gold_join_specs() -> None:
    tasks = load_tasks_from_task_set("benchmark/task_sets/synthetic_join_v1.json")

    for task in tasks:
        plan = load_gold(task, "plan")
        assert plan["joins"], f"{task['task_id']} should have at least one join spec"


def test_join_gold_plans_execute() -> None:
    tasks = load_tasks_from_task_set("benchmark/task_sets/synthetic_join_v1.json")

    for task in tasks:
        plan = load_gold(task, "plan")
        report = validate_execution(task, plan)
        assert report["valid"], f"{task['task_id']} failed gold execution: {report['errors']}"