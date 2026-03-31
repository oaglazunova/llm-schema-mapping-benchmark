from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from lsmbench.benchmark.task_builder import write_json
from lsmbench.generation.type_normalization_task_generator import build_type_normalization_specs


def _task_path(task_id: str) -> Path:
    return REPO_ROOT / "benchmark" / "tasks" / "synthetic" / "type_normalization" / f"{task_id}_task.json"


def _fixture_input_path(task_id: str) -> Path:
    return REPO_ROOT / "benchmark" / "fixtures" / "synthetic" / f"{task_id}_input.json"


def _fixture_expected_path(task_id: str) -> Path:
    return REPO_ROOT / "benchmark" / "fixtures" / "synthetic" / f"{task_id}_expected.json"


def _matches_path(task_id: str) -> Path:
    return REPO_ROOT / "benchmark" / "gold" / "matches" / f"{task_id}_matches.json"


def _plan_path(task_id: str) -> Path:
    return REPO_ROOT / "benchmark" / "gold" / "plans" / f"{task_id}_plan.json"


def _invariants_path(task_id: str) -> Path:
    return REPO_ROOT / "benchmark" / "gold" / "invariants" / f"{task_id}_invariants.json"


def build_tasks() -> list[str]:
    specs = build_type_normalization_specs()
    task_refs: list[str] = []

    for spec in specs:
        task_id = spec["task_id"]

        write_json(_fixture_input_path(task_id), spec["input_fixture"])
        write_json(_fixture_expected_path(task_id), spec["expected_fixture"])
        write_json(_matches_path(task_id), spec["matches"])
        write_json(_plan_path(task_id), spec["plan"])
        write_json(_invariants_path(task_id), spec["invariants"])

        task_obj = {
            "task_id": spec["task_id"],
            "title": spec["title"],
            "split": spec["split"],
            "difficulty": spec["difficulty"],
            "source_family": spec["source_family"],
            "target_entity": spec["target_entity"],
            "task_text": spec["task_text"],
            "primitive_family": spec["primitive_family"],
            "primitive_subtype": spec["primitive_subtype"],
            "lexical_perturbation": spec["lexical_perturbation"],
            "ambiguity_class": spec["ambiguity_class"],
            "composition_depth": spec["composition_depth"],
            "difficulty_axes": spec["difficulty_axes"],
            "source_schema": spec["source_schema"],
            "target_schema": spec["target_schema"],
            "fixture_refs": {
                "input": "../../../fixtures/synthetic/" + f"{task_id}_input.json",
                "expected": "../../../fixtures/synthetic/" + f"{task_id}_expected.json",
            },
            "gold_refs": {
                "matches": "../../../gold/matches/" + f"{task_id}_matches.json",
                "plan": "../../../gold/plans/" + f"{task_id}_plan.json",
                "invariants": "../../../gold/invariants/" + f"{task_id}_invariants.json",
            },
            "tags": spec["tags"],
            "notes": spec["notes"],
            "downstream_checks": spec["downstream_checks"],
        }

        write_json(_task_path(task_id), task_obj)
        task_refs.append(f"benchmark/tasks/synthetic/type_normalization/{task_id}_task.json")

    return task_refs


def build_task_set(task_refs: list[str]) -> None:
    out = {
        "name": "synthetic_type_normalization_v1",
        "version": "0.1.0",
        "tasks": task_refs,
        "notes": [
            "Primitive-first synthetic benchmark slice.",
            "Type normalization tasks only.",
        ],
    }
    write_json(REPO_ROOT / "benchmark" / "task_sets" / "synthetic_type_normalization_v1.json", out)


def main() -> int:
    task_refs = build_tasks()
    build_task_set(task_refs)
    print(f"Wrote {len(task_refs)} type-normalization tasks.")
    for ref in task_refs:
        print(f"  - {ref}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())