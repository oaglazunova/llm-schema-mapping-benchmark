from __future__ import annotations

import copy
import sys
from pathlib import Path

import pytest

# Make src/ importable when running pytest from repo root
REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from lsmbench.validators.pipeline import (
    load_json,
    load_task_bundle,
    validate_task_execution,
)


TASK_ROOT = REPO_ROOT / "benchmark" / "tasks" / "gamebus"
SCHEMA_PATH = REPO_ROOT / "schemas" / "mapping_plan_v1.schema.json"


def _schema() -> dict:
    return load_json(SCHEMA_PATH)


def _task_path(task_id: str) -> Path:
    return TASK_ROOT / f"{task_id}_task.json"


def _run(task_id: str, plan: dict):
    bundle = load_task_bundle(_task_path(task_id))
    return validate_task_execution(
        task=bundle["task"],
        plan=plan,
        fixture_input=bundle["fixture_input"],
        fixture_expected=bundle["fixture_expected"],
        invariants=bundle["invariants"],
        mapping_plan_schema=_schema(),
    )


def mutate_gb001_unknown_source_path(plan: dict) -> None:
    # Break a source reference so validation should fail before execution
    plan["field_mappings"][0]["source_paths"] = ["$.DOES_NOT_EXIST"]


def mutate_gb002_missing_required_field(plan: dict) -> None:
    # Remove production of a required target field
    plan["field_mappings"] = [
        fm for fm in plan["field_mappings"]
        if fm["target_field"] != "steps_sum"
    ]


def mutate_gb003_wrong_uri_source(plan: dict) -> None:
    # Still executable, but semantically wrong -> exact-output mismatch
    for fm in plan["field_mappings"]:
        if fm["target_field"] == "uri":
            fm["source_paths"] = ["$.APP"]
            return
    raise AssertionError("Could not find GB_003 uri mapping")


def mutate_gb004_raw_consent_items_not_canonical(plan: dict) -> None:
    # Revert the fix: parse the JSON array, but do not map tk->code / condition->condition_text
    for fm in plan["field_mappings"]:
        if fm["target_field"] == "consent_items":
            fm["operation"] = "parse_json_array"
            fm.pop("parameters", None)
            return
    raise AssertionError("Could not find GB_004 consent_items mapping")


def mutate_gb005_missing_key_parameter(plan: dict) -> None:
    # extract_kv_value requires the 'key' parameter
    for fm in plan["field_mappings"]:
        if fm["target_field"] == "gender":
            fm["parameters"] = {}
            return
    raise AssertionError("Could not find GB_005 gender mapping")


def mutate_gb006_wrong_points_source(plan: dict) -> None:
    # Still executable, but wrong -> exact-output mismatch
    for fm in plan["field_mappings"]:
        if fm["target_field"] == "points":
            fm["source_paths"] = ["$.ACTIVITY"]
            return
    raise AssertionError("Could not find GB_006 points mapping")


BROKEN_CASES = [
    (
        "GB_001",
        mutate_gb001_unknown_source_path,
        "V2_REFERENCES",
        "Unknown source path",
    ),
    (
        "GB_002",
        mutate_gb002_missing_required_field,
        "V3_STATIC",
        "Required target field is not produced",
    ),
    (
        "GB_003",
        mutate_gb003_wrong_uri_source,
        "V5_OUTPUT",
        "does not exactly match expected output",
    ),
    (
        "GB_004",
        mutate_gb004_raw_consent_items_not_canonical,
        "V5_OUTPUT",
        "does not exactly match expected output",
    ),
    (
        "GB_005",
        mutate_gb005_missing_key_parameter,
        "V3_STATIC",
        "requires parameter 'key'",
    ),
    (
        "GB_006",
        mutate_gb006_wrong_points_source,
        "V5_OUTPUT",
        "does not exactly match expected output",
    ),
]


@pytest.mark.parametrize(
    "task_id,mutator,expected_stage,expected_message_fragment",
    BROKEN_CASES,
)
def test_broken_plans_fail_for_expected_reason(
    task_id: str,
    mutator,
    expected_stage: str,
    expected_message_fragment: str,
) -> None:
    """
    For each GameBus anchor task, construct one intentionally broken plan
    and verify that validation fails for the expected reason.

    This is important because we do not only want 'gold plans pass';
    we also want 'bad plans are rejected in meaningful ways'.
    """
    bundle = load_task_bundle(_task_path(task_id))
    broken_plan = copy.deepcopy(bundle["gold_plan"])
    mutator(broken_plan)

    report = validate_task_execution(
        task=bundle["task"],
        plan=broken_plan,
        fixture_input=bundle["fixture_input"],
        fixture_expected=bundle["fixture_expected"],
        invariants=bundle["invariants"],
        mapping_plan_schema=_schema(),
    )

    assert not report.ok, f"{task_id} broken plan unexpectedly passed"

    assert any(
        issue.stage == expected_stage and expected_message_fragment in issue.message
        for issue in report.issues
    ), (
        f"{task_id} did fail, but not for the expected reason.\n"
        f"Expected stage={expected_stage!r}, message containing {expected_message_fragment!r}.\n"
        f"Actual issues: {report.issues}"
    )


def test_gb004_broken_plan_still_executes_but_fails_semantically() -> None:
    """
    GB_004 is a useful special case:
    the broken plan still parses and aggregates,
    but the produced consent_items structure is not canonical.

    This checks that the validator distinguishes:
      executable plans
      from
      correct plans
    """
    bundle = load_task_bundle(_task_path("GB_004"))
    broken_plan = copy.deepcopy(bundle["gold_plan"])
    mutate_gb004_raw_consent_items_not_canonical(broken_plan)

    report = validate_task_execution(
        task=bundle["task"],
        plan=broken_plan,
        fixture_input=bundle["fixture_input"],
        fixture_expected=bundle["fixture_expected"],
        invariants=bundle["invariants"],
        mapping_plan_schema=_schema(),
    )

    assert not report.ok
    assert report.produced_output is not None
    assert len(report.produced_output) == 1

    produced = report.produced_output[0]
    assert "consent_items" in produced
    assert isinstance(produced["consent_items"], list)
    assert len(produced["consent_items"]) > 0

    first_item = produced["consent_items"][0]

    # Broken output preserves raw provider keys instead of canonical benchmark keys
    assert "tk" in first_item
    assert "condition" in first_item
    assert "code" not in first_item
    assert "condition_text" not in first_item