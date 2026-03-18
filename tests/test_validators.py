from __future__ import annotations

import copy
import json
import sys
from pathlib import Path

# Make src/ importable when running pytest from repo root
REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from lsmbench.validators.pipeline import (
    load_json,
    load_task_bundle,
    validate_task_execution,
    validate_task_file,
)


TASK_IDS = ["GB_001", "GB_002", "GB_003", "GB_004", "GB_005", "GB_006"]


def mapping_plan_schema() -> dict:
    return load_json(REPO_ROOT / "schemas" / "mapping_plan_v1.schema.json")


def task_path(task_id: str) -> Path:
    return REPO_ROOT / "benchmark" / "tasks" / "gamebus" / f"{task_id}_task.json"


def test_all_gold_gamebus_tasks_pass() -> None:
    """
    The most important regression test:
    every shipped gold plan for GB_001–GB_006 must validate and execute successfully.
    """
    schema = mapping_plan_schema()

    for tid in TASK_IDS:
        bundle = load_task_bundle(task_path(tid))
        report = validate_task_execution(
            task=bundle["task"],
            plan=bundle["gold_plan"],
            fixture_input=bundle["fixture_input"],
            fixture_expected=bundle["fixture_expected"],
            invariants=bundle["invariants"],
            mapping_plan_schema=schema,
        )

        assert report.ok, f"{tid} should pass, but failed with issues: {report.issues}"


def test_validate_task_file_wrapper_works_for_gb004() -> None:
    """
    Smoke test for the convenience wrapper that loads the whole bundle from disk.
    GB_004 is the best canary because it exercises:
    - parse_json_array_map_fields
    - aggregations
    - invariants
    """
    report = validate_task_file(
        task_path=task_path("GB_004"),
        mapping_plan_schema_path=REPO_ROOT / "schemas" / "mapping_plan_v1.schema.json",
    )

    assert report.ok, f"GB_004 wrapper validation failed: {report.issues}"
    assert report.produced_output is not None
    assert len(report.produced_output) == 1


def test_unknown_source_path_fails_static_validation() -> None:
    """
    A broken plan that references a non-existent source field should fail
    before execution.
    """
    schema = mapping_plan_schema()
    bundle = load_task_bundle(task_path("GB_001"))

    broken_plan = copy.deepcopy(bundle["gold_plan"])
    broken_plan["field_mappings"][0]["source_paths"] = ["$.DOES_NOT_EXIST"]

    report = validate_task_execution(
        task=bundle["task"],
        plan=broken_plan,
        fixture_input=bundle["fixture_input"],
        fixture_expected=bundle["fixture_expected"],
        invariants=bundle["invariants"],
        mapping_plan_schema=schema,
    )

    assert not report.ok
    assert any(
        issue.stage == "V2_REFERENCES" and "Unknown source path" in issue.message
        for issue in report.issues
    ), report.issues


def test_missing_required_parameter_fails_static_validation() -> None:
    """
    extract_kv_value requires a 'key' parameter.
    Remove it and ensure validation fails at the static stage.
    """
    schema = mapping_plan_schema()
    bundle = load_task_bundle(task_path("GB_005"))

    broken_plan = copy.deepcopy(bundle["gold_plan"])

    # Remove required parameter from the gender extraction mapping
    for fm in broken_plan["field_mappings"]:
        if fm["target_field"] == "gender":
            fm["parameters"] = {}
            break

    report = validate_task_execution(
        task=bundle["task"],
        plan=broken_plan,
        fixture_input=bundle["fixture_input"],
        fixture_expected=bundle["fixture_expected"],
        invariants=bundle["invariants"],
        mapping_plan_schema=schema,
    )

    assert not report.ok
    assert any(
        issue.stage == "V3_STATIC" and "requires parameter 'key'" in issue.message
        for issue in report.issues
    ), report.issues


def test_required_target_field_missing_fails_static_validation() -> None:
    """
    Remove one required output field from the gold plan and verify
    the static validator catches it.
    """
    schema = mapping_plan_schema()
    bundle = load_task_bundle(task_path("GB_002"))

    broken_plan = copy.deepcopy(bundle["gold_plan"])
    broken_plan["field_mappings"] = [
        fm for fm in broken_plan["field_mappings"]
        if fm["target_field"] != "steps_sum"
    ]

    report = validate_task_execution(
        task=bundle["task"],
        plan=broken_plan,
        fixture_input=bundle["fixture_input"],
        fixture_expected=bundle["fixture_expected"],
        invariants=bundle["invariants"],
        mapping_plan_schema=schema,
    )

    assert not report.ok
    assert any(
        issue.stage == "V3_STATIC" and "Required target field is not produced" in issue.message
        for issue in report.issues
    ), report.issues


def test_wrong_output_fails_exact_output_check() -> None:
    """
    A semantically wrong but executable plan should reach execution and then fail
    on exact output comparison.
    """
    schema = mapping_plan_schema()
    bundle = load_task_bundle(task_path("GB_006"))

    broken_plan = copy.deepcopy(bundle["gold_plan"])

    # Make points come from the wrong source field but keep it executable
    for fm in broken_plan["field_mappings"]:
        if fm["target_field"] == "points":
            fm["source_paths"] = ["$.ACTIVITY"]
            break

    report = validate_task_execution(
        task=bundle["task"],
        plan=broken_plan,
        fixture_input=bundle["fixture_input"],
        fixture_expected=bundle["fixture_expected"],
        invariants=bundle["invariants"],
        mapping_plan_schema=schema,
    )

    assert not report.ok
    assert any(
        issue.stage == "V5_OUTPUT" and "does not exactly match expected output" in issue.message
        for issue in report.issues
    ), report.issues


def test_gb004_produces_canonical_consent_item_keys() -> None:
    """
    Regression test for the GB_004 fix:
    consent_items must use canonical keys:
      - code
      - condition_text
      - accepted
    not raw provider keys like 'tk' and 'condition'.
    """
    schema = mapping_plan_schema()
    bundle = load_task_bundle(task_path("GB_004"))

    report = validate_task_execution(
        task=bundle["task"],
        plan=bundle["gold_plan"],
        fixture_input=bundle["fixture_input"],
        fixture_expected=bundle["fixture_expected"],
        invariants=bundle["invariants"],
        mapping_plan_schema=schema,
    )

    assert report.ok, report.issues
    produced = report.produced_output
    assert produced is not None
    assert len(produced) == 1

    consent_items = produced[0]["consent_items"]
    assert isinstance(consent_items, list)
    assert len(consent_items) > 0

    first_item = consent_items[0]
    assert "code" in first_item
    assert "condition_text" in first_item
    assert "accepted" in first_item
    assert "tk" not in first_item
    assert "condition" not in first_item