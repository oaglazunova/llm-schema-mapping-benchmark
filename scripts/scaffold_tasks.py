# How to use:
# python scripts/scaffold_task.py ^
#   --task-id PUB_107 ^
#   --title "Map bundled shipment events to shipment_summary" ^
#   --split public ^
#   --difficulty medium ^
#   --source-family openapi_shipment_event ^
#   --target-entity shipment_summary ^
#   --task-text "Create one shipment_summary JSON object from shipment event data." ^
#   --input-fixture benchmark/fixtures/public/PUB_107_input.json ^
#   --expected-fixture benchmark/fixtures/public/PUB_107_expected.json ^
#   --tags public logistics shipping



from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from lsmbench.benchmark.task_builder import (
    build_empty_invariants,
    build_empty_matches,
    build_empty_plan,
    build_task_skeleton,
    first_record_from_fixture,
    infer_schema_from_example,
    load_fixture,
    write_json,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scaffold a benchmark task from input and expected fixture files."
    )
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--title", required=True)
    parser.add_argument("--split", required=True, choices=["gamebus", "public", "synthetic"])
    parser.add_argument("--difficulty", required=True, choices=["easy", "medium", "hard"])
    parser.add_argument("--source-family", required=True)
    parser.add_argument("--target-entity", required=True)
    parser.add_argument("--task-text", required=True)
    parser.add_argument("--input-fixture", required=True, help="Repo-relative path to input fixture")
    parser.add_argument("--expected-fixture", required=True, help="Repo-relative path to expected fixture")
    parser.add_argument("--tags", nargs="*", default=[])
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def ensure_not_exists(path: Path, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise FileExistsError(
            f"Refusing to overwrite existing file: {path}. Use --overwrite to allow it."
        )


def main() -> int:
    args = parse_args()

    input_fixture_path = REPO_ROOT / args.input_fixture
    expected_fixture_path = REPO_ROOT / args.expected_fixture

    input_fixture = load_fixture(input_fixture_path)
    expected_fixture = load_fixture(expected_fixture_path)

    source_example = first_record_from_fixture(input_fixture)
    target_example = first_record_from_fixture(expected_fixture)

    source_schema = infer_schema_from_example(source_example, title=f"{args.task_id}SourceRecord")
    target_schema = infer_schema_from_example(target_example, title=f"{args.task_id}TargetRecord")

    task = build_task_skeleton(
        task_id=args.task_id,
        title=args.title,
        split=args.split,
        difficulty=args.difficulty,
        source_family=args.source_family,
        target_entity=args.target_entity,
        task_text=args.task_text,
        source_schema=source_schema,
        target_schema=target_schema,
        input_fixture_ref=args.input_fixture,
        expected_fixture_ref=args.expected_fixture,
        tags=args.tags,
        notes=["Scaffolded by scripts/scaffold_task.py"],
    )

    matches = build_empty_matches()
    plan = build_empty_plan(task_id=args.task_id, target_entity=args.target_entity)
    invariants = build_empty_invariants()

    task_path = REPO_ROOT / "benchmark" / "tasks" / args.split / f"{args.task_id}_task.json"
    matches_path = REPO_ROOT / "benchmark" / "gold" / "matches" / f"{args.task_id}_matches.json"
    plan_path = REPO_ROOT / "benchmark" / "gold" / "plans" / f"{args.task_id}_plan.json"
    invariants_path = REPO_ROOT / "benchmark" / "gold" / "invariants" / f"{args.task_id}_invariants.json"

    for path in [task_path, matches_path, plan_path, invariants_path]:
        ensure_not_exists(path, args.overwrite)

    write_json(task_path, task)
    write_json(matches_path, matches)
    write_json(plan_path, plan)
    write_json(invariants_path, invariants)

    print("Scaffolded:")
    print(f"  - {task_path.relative_to(REPO_ROOT)}")
    print(f"  - {matches_path.relative_to(REPO_ROOT)}")
    print(f"  - {plan_path.relative_to(REPO_ROOT)}")
    print(f"  - {invariants_path.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())