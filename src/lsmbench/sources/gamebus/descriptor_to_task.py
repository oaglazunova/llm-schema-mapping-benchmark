from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from lsmbench.config.paths import (
    GAMEBUS_TASKS_DIR,
    GAMEBUS_FIXTURES_DIR,
    GOLD_MATCHES_DIR,
    GOLD_PLANS_DIR,
    GOLD_INVARIANTS_DIR,
    ensure_dir,
)


KNOWN_DESCRIPTOR_SPECS: dict[str, dict[str, Any]] = {
    "nutrition_summary": {
        "task_id": "GB_001",
        "title": "Map NUTRITION_SUMMARY to daily_nutrition_summary",
        "difficulty": "easy",
        "target_entity": "daily_nutrition_summary",
        "task_text": "Map one GameBus NUTRITION_SUMMARY record into one normalized daily nutrition summary object.",
        "target_fields": [
            {"name": "source_activity_id", "type": "integer", "required": True},
            {"name": "date", "type": "date", "required": True},
            {"name": "aggregation_level", "type": "string", "required": True},
            {"name": "summary_score", "type": "integer", "required": True},
            {"name": "carbs_conformance", "type": "integer", "required": True},
            {"name": "fat_conformance", "type": "integer", "required": True},
            {"name": "fiber_conformance", "type": "integer", "required": True},
        ],
        "mappings": [
            {"target_field": "source_activity_id", "operation": "copy", "source_paths": ["$.X_ACTIVITY_ID"]},
            {"target_field": "date", "operation": "truncate_date", "source_paths": ["$.X_DATE"]},
            {"target_field": "aggregation_level", "operation": "copy", "source_paths": ["$.AGGREGATION_LEVEL"]},
            {"target_field": "summary_score", "operation": "cast_integer", "source_paths": ["$.SUMMARY_SCORE"]},
            {"target_field": "carbs_conformance", "operation": "cast_integer", "source_paths": ["$.CARBS_CONFORMANCE"]},
            {"target_field": "fat_conformance", "operation": "cast_integer", "source_paths": ["$.FAT_CONFORMANCE"]},
            {"target_field": "fiber_conformance", "operation": "cast_integer", "source_paths": ["$.FIBER_CONFORMANCE"]},
        ],
        "aggregations": [],
        "invariants": [
            "date is normalized to YYYY-MM-DD",
            "summary_score is an integer",
            "aggregation_level is one of DAILY or WEEKLY",
        ],
    },
    "day_aggregate": {
        "task_id": "GB_002",
        "title": "Map DAY_AGGREGATE to daily_activity_summary",
        "difficulty": "easy",
        "target_entity": "daily_activity_summary",
        "task_text": "Map one GameBus DAY_AGGREGATE record into one normalized daily activity summary object.",
        "target_fields": [
            {"name": "source_activity_id", "type": "integer", "required": True},
            {"name": "date", "type": "date", "required": True},
            {"name": "steps_sum", "type": "integer", "required": True},
            {"name": "distance_sum_m", "type": "number", "required": True},
            {"name": "pa_cal_sum_kcal", "type": "integer", "required": True},
        ],
        "mappings": [
            {"target_field": "source_activity_id", "operation": "copy", "source_paths": ["$.X_ACTIVITY_ID"]},
            {"target_field": "date", "operation": "truncate_date", "source_paths": ["$.X_DATE"]},
            {"target_field": "steps_sum", "operation": "cast_integer", "source_paths": ["$.STEPS_SUM"]},
            {"target_field": "distance_sum_m", "operation": "cast_number", "source_paths": ["$.DISTANCE_SUM"]},
            {"target_field": "pa_cal_sum_kcal", "operation": "cast_integer", "source_paths": ["$.PA_CAL_SUM"]},
        ],
        "aggregations": [],
        "invariants": [
            "steps_sum >= 0",
            "distance_sum_m >= 0",
            "pa_cal_sum_kcal >= 0",
        ],
    },
    "navigate_app": {
        "task_id": "GB_003",
        "title": "Map NAVIGATE_APP to navigation_event",
        "difficulty": "easy",
        "target_entity": "navigation_event",
        "task_text": "Map one NAVIGATE_APP event into one normalized navigation_event object.",
        "target_fields": [
            {"name": "event_time", "type": "datetime", "required": True},
            {"name": "source_activity_id", "type": "integer", "required": True},
            {"name": "app", "type": "string", "required": True},
            {"name": "session_id", "type": "string", "required": True},
            {"name": "user_external_id", "type": "string", "required": True},
            {"name": "campaign", "type": "string", "required": False},
            {"name": "uri", "type": "string", "required": True},
        ],
        "mappings": [
            {"target_field": "event_time", "operation": "parse_datetime", "source_paths": ["$.X_DATE"]},
            {"target_field": "source_activity_id", "operation": "copy", "source_paths": ["$.X_ACTIVITY_ID"]},
            {"target_field": "app", "operation": "copy", "source_paths": ["$.APP"]},
            {"target_field": "session_id", "operation": "copy", "source_paths": ["$.SESSION"]},
            {"target_field": "user_external_id", "operation": "copy", "source_paths": ["$.UID"]},
            {"target_field": "campaign", "operation": "copy", "source_paths": ["$.FOR_CAMPAIGN"]},
            {"target_field": "uri", "operation": "copy", "source_paths": ["$.URI"]},
        ],
        "aggregations": [],
        "invariants": [
            "event_time parses as datetime",
            "session_id is non-empty",
            "uri is non-empty",
        ],
    },
    "consent": {
        "task_id": "GB_004",
        "title": "Map CONSENT to consent_record",
        "difficulty": "medium",
        "target_entity": "consent_record",
        "task_text": "Parse the JSON string in DESCRIPTION and produce a normalized consent record.",
        "target_fields": [
            {"name": "consent_time", "type": "datetime", "required": True},
            {"name": "source_activity_id", "type": "integer", "required": True},
            {"name": "campaign", "type": "string", "required": True},
            {"name": "consent_items", "type": "array", "required": True},
            {"name": "accepted_count", "type": "integer", "required": True},
            {"name": "item_count", "type": "integer", "required": True},
            {"name": "all_required_accepted", "type": "boolean", "required": True},
        ],
        "mappings": [
            {"target_field": "consent_time", "operation": "parse_datetime", "source_paths": ["$.X_DATE"]},
            {"target_field": "source_activity_id", "operation": "copy", "source_paths": ["$.X_ACTIVITY_ID"]},
            {"target_field": "campaign", "operation": "copy", "source_paths": ["$.FOR_CAMPAIGN"]},
            {"target_field": "consent_items", "operation": "parse_json_array", "source_paths": ["$.DESCRIPTION"]},
        ],
        "aggregations": [
            {
                "target_field": "accepted_count",
                "function": "count_true",
                "source_path": "$.consent_items[*].accepted",
            },
            {
                "target_field": "item_count",
                "function": "count",
                "source_path": "$.consent_items[*]",
            },
            {
                "target_field": "all_required_accepted",
                "function": "all_true",
                "source_path": "$.consent_items[*].accepted",
            },
        ],
        "invariants": [
            "consent_items is a non-empty array",
            "accepted_count <= item_count",
            "all_required_accepted is boolean",
        ],
    },
    "general_survey": {
        "task_id": "GB_005",
        "title": "Map GENERAL_SURVEY to general_survey_profile",
        "difficulty": "medium",
        "target_entity": "general_survey_profile",
        "task_text": "Extract typed fields such as gender and age from DESCRIPTION key:value arrays.",
        "target_fields": [
            {"name": "survey_time", "type": "datetime", "required": True},
            {"name": "source_activity_id", "type": "integer", "required": True},
            {"name": "gender", "type": "string", "required": False},
            {"name": "age", "type": "integer", "required": False},
            {"name": "raw_pairs", "type": "array", "required": True},
        ],
        "mappings": [
            {"target_field": "survey_time", "operation": "parse_datetime", "source_paths": ["$.X_DATE"]},
            {"target_field": "source_activity_id", "operation": "copy", "source_paths": ["$.X_ACTIVITY_ID"]},
            {"target_field": "raw_pairs", "operation": "copy", "source_paths": ["$.DESCRIPTION"]},
            {
                "target_field": "gender",
                "operation": "extract_kv_value",
                "source_paths": ["$.DESCRIPTION"],
                "parameters": {"key": "gender", "delimiter": ":"},
            },
            {
                "target_field": "age",
                "operation": "extract_kv_value_cast_integer",
                "source_paths": ["$.DESCRIPTION"],
                "parameters": {"key": "age", "delimiter": ":"},
            },
        ],
        "aggregations": [],
        "invariants": [
            "age is integer when present",
            "raw_pairs preserves original DESCRIPTION",
        ],
    },
    "score_gamebus_points": {
        "task_id": "GB_006",
        "title": "Map SCORE_GAMEBUS_POINTS to points_event",
        "difficulty": "medium",
        "target_entity": "points_event",
        "task_text": "Normalize a GameBus points-award event into a canonical points_event object.",
        "target_fields": [
            {"name": "event_time", "type": "datetime", "required": True},
            {"name": "source_activity_id", "type": "integer", "required": True},
            {"name": "challenge_id", "type": "integer", "required": True},
            {"name": "challenge_rule_id", "type": "integer", "required": True},
            {"name": "points", "type": "integer", "required": True},
            {"name": "trigger_activity_id", "type": "integer", "required": True},
        ],
        "mappings": [
            {"target_field": "event_time", "operation": "parse_datetime", "source_paths": ["$.X_DATE"]},
            {"target_field": "source_activity_id", "operation": "copy", "source_paths": ["$.X_ACTIVITY_ID"]},
            {"target_field": "challenge_id", "operation": "cast_integer", "source_paths": ["$.FOR_CHALLENGE"]},
            {"target_field": "challenge_rule_id", "operation": "cast_integer", "source_paths": ["$.CHALLENGE_RULE"]},
            {"target_field": "points", "operation": "cast_integer", "source_paths": ["$.NUMBER_OF_POINTS"]},
            {"target_field": "trigger_activity_id", "operation": "cast_integer", "source_paths": ["$.ACTIVITY"]},
        ],
        "aggregations": [],
        "invariants": [
            "points > 0",
            "all id fields are integers",
        ],
    },
    "tizen(detail)": {
        "task_id": "GB_007",
        "title": "Map TIZEN(detail) to sensor_activity_detail",
        "difficulty": "hard",
        "target_entity": "sensor_activity_detail",
        "task_text": "Parse the Python-like object stored in ACTIVITY_TYPE and expose its structured fields.",
        "target_fields": [
            {"name": "event_time", "type": "datetime", "required": True},
            {"name": "source_activity_id", "type": "integer", "required": True},
            {"name": "activity_type_raw", "type": "string", "required": False},
            {"name": "activity_type_obj", "type": "object", "required": False},
        ],
        "mappings": [
            {"target_field": "event_time", "operation": "parse_datetime", "source_paths": ["$.X_DATE"]},
            {"target_field": "source_activity_id", "operation": "copy", "source_paths": ["$.X_ACTIVITY_ID"]},
            {"target_field": "activity_type_raw", "operation": "copy", "source_paths": ["$.ACTIVITY_TYPE"]},
            {"target_field": "activity_type_obj", "operation": "parse_pythonish_object", "source_paths": ["$.ACTIVITY_TYPE"]},
        ],
        "aggregations": [],
        "invariants": [
            "activity_type_obj is either null or an object",
            "event_time parses as datetime",
        ],
    },
}


def _load_json(path: Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _load_raw_source_records(source_file: str) -> list[dict[str, Any]]:
    path = Path(source_file)
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        return [data]
    return []


def _source_field_to_schema(field_profile: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": field_profile["field_name"],
        "raw_type_counts": field_profile["raw_type_counts"],
        "logical_type_hints": field_profile["logical_type_hints"],
        "recommended_operations": field_profile["recommended_operations"],
        "examples": field_profile["example_values"],
        "null_count": field_profile["null_count"],
    }


def _generic_target_field(field_profile: dict[str, Any]) -> dict[str, Any]:
    field_name = field_profile["field_name"].lower()
    hints = set(field_profile.get("logical_type_hints", []))

    if "datetime_string" in hints:
        typ = "datetime"
    elif "integer_string" in hints:
        typ = "integer"
    elif "numeric_string" in hints:
        typ = "number"
    elif "json_array_string" in hints or "kv_array" in hints:
        typ = "array"
    elif "json_object_string" in hints or "pythonish_object_string" in hints:
        typ = "object"
    else:
        typ = "string"

    return {"name": field_name, "type": typ, "required": field_profile["null_count"] == 0}


def _build_generic_spec(profile: dict[str, Any]) -> dict[str, Any]:
    descriptor_name = profile["descriptor_name"]
    target_entity = f"{descriptor_name}_record".replace("(", "_").replace(")", "_").replace(" ", "_")

    target_fields = [_generic_target_field(fp) for fp in profile["field_profiles"]]
    mappings = []
    for fp in profile["field_profiles"]:
        op = fp["recommended_operations"][0] if fp["recommended_operations"] else "copy"
        mappings.append(
            {
                "target_field": fp["field_name"].lower(),
                "operation": op,
                "source_paths": [f"$.{fp['field_name']}"],
            }
        )

    return {
        "task_id": "GB_TODO",
        "title": f"Map {descriptor_name} to {target_entity}",
        "difficulty": "medium",
        "target_entity": target_entity,
        "task_text": f"Map one {descriptor_name} record into a normalized target object.",
        "target_fields": target_fields,
        "mappings": mappings,
        "aggregations": [],
        "invariants": [],
    }


def build_task_bundle_from_profile(profile_path: str | Path) -> dict[str, Path]:
    profile_path = Path(profile_path)
    profile = _load_json(profile_path)

    descriptor_name = profile["descriptor_name"]
    spec = KNOWN_DESCRIPTOR_SPECS.get(descriptor_name, _build_generic_spec(profile))

    task_id = spec["task_id"]

    ensure_dir(GAMEBUS_TASKS_DIR)
    ensure_dir(GAMEBUS_FIXTURES_DIR)
    ensure_dir(GOLD_MATCHES_DIR)
    ensure_dir(GOLD_PLANS_DIR)
    ensure_dir(GOLD_INVARIANTS_DIR)

    source_schema = {
        "descriptor_name": descriptor_name,
        "top_level_type": profile["top_level_type"],
        "record_count": profile["record_count"],
        "field_count": profile["field_count"],
        "benchmark_family_hint": profile["benchmark_family_hint"],
        "fields": [_source_field_to_schema(fp) for fp in profile["field_profiles"]],
    }

    target_schema = {
        "target_entity": spec["target_entity"],
        "fields": spec["target_fields"],
    }

    task_payload = {
        "task_id": task_id,
        "title": spec["title"],
        "difficulty": spec["difficulty"],
        "source_family": "gamebus",
        "source_file": profile["source_file"],
        "descriptor_name": descriptor_name,
        "target_entity": spec["target_entity"],
        "benchmark_family_hint": profile["benchmark_family_hint"],
        "task_text": spec["task_text"],
        "source_schema": source_schema,
        "target_schema": target_schema,
        "notes": [
            "Auto-generated from descriptor profile. Review before release.",
            "Fixtures are samples only. Expected outputs may still need manual refinement."
        ]
    }

    gold_matches = [
        {
            "target_field": m["target_field"],
            "source_paths": m["source_paths"],
            "operation": m["operation"],
        }
        for m in spec["mappings"]
    ]

    gold_plan = {
        "plan_id": f"{task_id}_PLAN",
        "task_id": task_id,
        "target_entity": spec["target_entity"],
        "field_mappings": spec["mappings"],
        "joins": [],
        "filters": [],
        "aggregations": spec["aggregations"],
        "assumptions": ["Auto-generated from known descriptor template."],
        "confidence": 1.0,
    }

    invariants = {
        "task_id": task_id,
        "invariants": spec["invariants"],
    }

    raw_records = _load_raw_source_records(profile["source_file"])
    fixture_sample = raw_records[: min(5, len(raw_records))]

    task_path = GAMEBUS_TASKS_DIR / f"{task_id}_task.json"
    matches_path = GOLD_MATCHES_DIR / f"{task_id}_matches.json"
    plan_path = GOLD_PLANS_DIR / f"{task_id}_plan.json"
    invariants_path = GOLD_INVARIANTS_DIR / f"{task_id}_invariants.json"
    fixture_path = GAMEBUS_FIXTURES_DIR / f"{task_id}_input.sample.json"

    for path, payload in [
        (task_path, task_payload),
        (matches_path, gold_matches),
        (plan_path, gold_plan),
        (invariants_path, invariants),
        (fixture_path, fixture_sample),
    ]:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)

    return {
        "task": task_path,
        "matches": matches_path,
        "plan": plan_path,
        "invariants": invariants_path,
        "fixture": fixture_path,
    }