from __future__ import annotations

from lsmbench.benchmark.task_loader import load_tasks_from_task_set


def test_synthetic_type_normalization_task_set_loads() -> None:
    tasks = load_tasks_from_task_set("benchmark/task_sets/synthetic_type_normalization_v1.json")
    assert tasks

    for task in tasks:
        assert task["primitive_family"] == "type_normalization"
        assert task["composition_depth"] == 1
        assert task["split"] == "synthetic"
        assert task["source_family"] == "synthetic_type_normalization"


def test_type_normalization_tasks_use_expected_operations() -> None:
    tasks = load_tasks_from_task_set("benchmark/task_sets/synthetic_type_normalization_v1.json")
    allowed = {
        "cast_integer",
        "parse_date",
        "normalize_boolean",
        "normalize_enum",
        "truncate_date",
    }

    for task in tasks:
        assert task["primitive_subtype"] in allowed