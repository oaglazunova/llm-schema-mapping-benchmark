from __future__ import annotations

from lsmbench.benchmark.task_loader import load_tasks_from_task_set


def _source_fields(task: dict) -> set[str]:
    props = task["source_schema"]["properties"]
    return set(props.keys())


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


def test_copy_rename_task_set_uses_concept_specific_variants() -> None:
    tasks = load_tasks_from_task_set("benchmark/task_sets/synthetic_copy_rename_v1.json")

    birth_date_family = {
        "birth_date",
        "date_of_birth",
        "dob",
        "date",
        "fecha_nacimiento",
        "created_date",
    }
    steps_family = {
        "steps",
        "step_count",
        "step_cnt",
        "activity_count",
        "pasos",
        "distance_meters",
    }

    for task in tasks:
        src_fields = _source_fields(task)

        if task["target_entity"] == "canonical_user_profile":
            assert set(task["target_schema"]["properties"].keys()) == {"birth_date"}
            assert src_fields <= birth_date_family, (
                f"{task['task_id']} mixes non-birth-date source labels: {src_fields}"
            )

        elif task["target_entity"] == "canonical_activity_summary":
            assert set(task["target_schema"]["properties"].keys()) == {"steps"}
            assert src_fields <= steps_family, (
                f"{task['task_id']} mixes non-steps source labels: {src_fields}"
            )


def test_distractor_tasks_have_extra_source_field() -> None:
    tasks = load_tasks_from_task_set("benchmark/task_sets/synthetic_copy_rename_v1.json")

    distractor_tasks = [t for t in tasks if t["lexical_perturbation"] == "distractor"]
    assert distractor_tasks, "Expected at least one distractor task."

    for task in distractor_tasks:
        assert len(_source_fields(task)) > 1, (
            f"{task['task_id']} is marked as distractor but has only one source field"
        )