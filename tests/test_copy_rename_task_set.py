from __future__ import annotations

from lsmbench.benchmark.task_loader import load_tasks_from_task_set


def test_synthetic_copy_rename_task_set_loads() -> None:
    tasks = load_tasks_from_task_set("benchmark/task_sets/synthetic_copy_rename_v1.json")
    assert tasks, "Expected at least one generated copy/rename task."

    for task in tasks:
        assert task["primitive_family"] == "copy_rename"
        assert task["composition_depth"] == 1
        assert task["split"] == "synthetic"
        assert task["lexical_perturbation"] in {
            "exact",
            "synonym",
            "abbreviation",
            "hypernym",
            "multilingual",
            "distractor",
        }