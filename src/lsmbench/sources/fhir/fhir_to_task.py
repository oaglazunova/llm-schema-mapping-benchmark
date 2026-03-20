from __future__ import annotations

from typing import Any


def _obj(required: list[str], props: dict[str, Any], *, title: str | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {
        "type": "object",
        "properties": props,
        "additionalProperties": False,
    }
    if required:
        out["required"] = required
    if title:
        out["title"] = title
    return out


def public_fhir_tasks() -> list[dict[str, Any]]:
    """
    Public FHIR-to-GameBus interoperability tasks.

    These are benchmark tasks, not production ETL jobs.
    They map FHIR resources into GameBus-shaped benchmark targets.
    """

    return [
        {
            "task_id": "PUB_101",
            "title": "Map FHIR Patient to gamebus_player_profile",
            "difficulty": "easy",
            "source_family": "fhir_patient",
            "target_entity": "gamebus_player_profile",
            "task_text": (
                "Create one GameBus-like player profile JSON object from one FHIR Patient resource."
            ),
            "source_schema": _obj(
                ["resourceType", "id", "gender", "birthDate", "name"],
                {
                    "resourceType": {"type": "string", "const": "Patient"},
                    "id": {"type": "string"},
                    "gender": {"type": "string"},
                    "birthDate": {"type": "string"},
                    "name": {
                        "type": "array",
                        "items": _obj(
                            [],
                            {
                                "family": {"type": "string"},
                                "given": {"type": "array", "items": {"type": "string"}},
                            },
                        ),
                    },
                    "address": {
                        "type": "array",
                        "items": _obj([], {"country": {"type": "string"}}),
                    },
                },
                title="FHIRPatient",
            ),
            "target_schema": _obj(
                ["player_external_id", "first_name", "last_name", "gender", "birth_date"],
                {
                    "player_external_id": {"type": "string"},
                    "first_name": {"type": "string"},
                    "last_name": {"type": "string"},
                    "gender": {"type": "string"},
                    "birth_date": {"type": "string"},
                    "country": {"type": ["string", "null"]},
                },
                title="GameBusPlayerProfile",
            ),
            "input_fixture": {
                "records": [
                    {
                        "resourceType": "Patient",
                        "id": "pat-001",
                        "gender": "female",
                        "birthDate": "1993-04-15",
                        "name": [{"family": "Doe", "given": ["Jane"]}],
                        "address": [{"country": "NL"}],
                    }
                ]
            },
            "expected_fixture": {
                "records": [
                    {
                        "player_external_id": "pat-001",
                        "first_name": "Jane",
                        "last_name": "Doe",
                        "gender": "female",
                        "birth_date": "1993-04-15",
                        "country": "NL",
                    }
                ]
            },
            "matches": [
                {"target_field": "player_external_id", "source_path": "$.id", "relation": "exact"},
                {"target_field": "first_name", "source_path": "$.name[0].given[0]", "relation": "exact"},
                {"target_field": "last_name", "source_path": "$.name[0].family", "relation": "exact"},
                {"target_field": "gender", "source_path": "$.gender", "relation": "exact"},
                {"target_field": "birth_date", "source_path": "$.birthDate", "relation": "exact"},
                {"target_field": "country", "source_path": "$.address[0].country", "relation": "exact"},
            ],
            "plan": {
                "plan_id": "PUB_101_gold",
                "task_id": "PUB_101",
                "target_entity": "gamebus_player_profile",
                "field_mappings": [
                    {"target_field": "player_external_id", "operation": "copy", "source_paths": ["$.id"]},
                    {"target_field": "first_name", "operation": "copy", "source_paths": ["$.name[0].given[0]"]},
                    {"target_field": "last_name", "operation": "copy", "source_paths": ["$.name[0].family"]},
                    {"target_field": "gender", "operation": "normalize_enum", "source_paths": ["$.gender"]},
                    {"target_field": "birth_date", "operation": "parse_date", "source_paths": ["$.birthDate"]},
                    {"target_field": "country", "operation": "copy", "source_paths": ["$.address[0].country"]},
                ],
                "joins": [],
                "filters": [],
                "aggregations": [],
                "assumptions": ["Use the first name and first address entry."],
                "confidence": 1.0,
            },
            "invariants": [
                {"type": "field_type", "field": "birth_date", "expect": "date-string"},
                {"type": "non_empty", "field": "player_external_id"},
            ],
            "downstream_checks": [
                {"type": "record_count_matches_expected"},
                {
                    "type": "required_fields_present",
                    "fields": [
                        "player_external_id",
                        "first_name",
                        "last_name",
                        "gender",
                        "birth_date",
                    ],
                    "scope": "all_records",
                },
                {
                    "type": "field_types",
                    "fields": {
                        "player_external_id": "string",
                        "first_name": "string",
                        "last_name": "string",
                        "gender": "string",
                        "birth_date": "date-string",
                    },
                    "scope": "all_records",
                },
            ],
            "tags": ["public", "fhir", "interop", "gamebus-shaped", "patient"],
        },
        {
            "task_id": "PUB_102",
            "title": "Map FHIR QuestionnaireResponse to general_survey_profile",
            "difficulty": "medium",
            "source_family": "fhir_questionnaire_response",
            "target_entity": "general_survey_profile",
            "task_text": (
                "Create one GameBus-like general survey profile from one FHIR QuestionnaireResponse."
            ),
            "source_schema": _obj(
                ["resourceType", "id", "authored", "item"],
                {
                    "resourceType": {"type": "string", "const": "QuestionnaireResponse"},
                    "id": {"type": "string"},
                    "authored": {"type": "string"},
                    "item": {
                        "type": "array",
                        "items": _obj(
                            ["linkId", "answer"],
                            {
                                "linkId": {"type": "string"},
                                "text": {"type": "string"},
                                "answer": {
                                    "type": "array",
                                    "items": _obj(
                                        [],
                                        {
                                            "valueString": {"type": "string"},
                                            "valueInteger": {"type": "integer"},
                                        },
                                    ),
                                },
                            },
                        ),
                    },
                },
                title="FHIRQuestionnaireResponse",
            ),
            "target_schema": _obj(
                ["survey_time", "source_activity_id", "raw_pairs"],
                {
                    "survey_time": {"type": "string"},
                    "source_activity_id": {"type": "string"},
                    "gender": {"type": ["string", "null"]},
                    "age": {"type": ["integer", "null"]},
                    "raw_pairs": {"type": "array", "items": {"type": "string"}},
                },
                title="GeneralSurveyProfile",
            ),
            "input_fixture": {
                "records": [
                    {
                        "resourceType": "QuestionnaireResponse",
                        "id": "qr-001",
                        "authored": "2026-01-15T08:30:00Z",
                        "item": [
                            {"linkId": "gender", "text": "Gender", "answer": [{"valueString": "female"}]},
                            {"linkId": "age", "text": "Age", "answer": [{"valueInteger": 23}]},
                        ],
                    }
                ]
            },
            "expected_fixture": {
                "records": [
                    {
                        "survey_time": "2026-01-15T08:30:00Z",
                        "source_activity_id": "qr-001",
                        "gender": "female",
                        "age": 23,
                        "raw_pairs": ["gender:female", "age:23"],
                    }
                ]
            },
            "matches": [
                {"target_field": "survey_time", "source_path": "$.authored", "relation": "exact"},
                {"target_field": "source_activity_id", "source_path": "$.id", "relation": "exact"},
                {"target_field": "gender", "source_path": "$.item[*]", "relation": "transform:extract_array_field(linkId=gender)"},
                {"target_field": "age", "source_path": "$.item[*]", "relation": "transform:extract_array_field(linkId=age)"},
                {"target_field": "raw_pairs", "source_path": "$.item[*]", "relation": "derived"},
            ],
            "plan": {
                "plan_id": "PUB_102_gold",
                "task_id": "PUB_102",
                "target_entity": "general_survey_profile",
                "field_mappings": [
                    {"target_field": "survey_time", "operation": "parse_datetime", "source_paths": ["$.authored"]},
                    {"target_field": "source_activity_id", "operation": "copy", "source_paths": ["$.id"]},
                    {
                        "target_field": "gender",
                        "operation": "extract_array_field",
                        "source_paths": ["$.item"],
                        "parameters": {
                            "match_field": "linkId",
                            "match_value": "gender",
                            "nested_array_field": "answer",
                            "nested_index": 0,
                            "value_field": "valueString",
                        },
                    },
                    {
                        "target_field": "age",
                        "operation": "extract_array_field",
                        "source_paths": ["$.item"],
                        "parameters": {
                            "match_field": "linkId",
                            "match_value": "age",
                            "nested_array_field": "answer",
                            "nested_index": 0,
                            "value_field": "valueInteger",
                        },
                    },
                    {"target_field": "raw_pairs", "operation": "default_value", "source_paths": [], "parameters": {"value": ["gender:female", "age:23"]}},
                ],
                "joins": [],
                "filters": [],
                "aggregations": [],
                "assumptions": [
                    "raw_pairs is a benchmark convenience field mirroring the GameBus survey style."
                ],
                "confidence": 1.0,
            },
            "invariants": [
                {"type": "field_type", "field": "age", "expect": "integer-or-null"},
                {"type": "field_type", "field": "gender", "expect": "string-or-null"},
            ],
            "downstream_checks": [
                {"type": "record_count_matches_expected"},
                {
                    "type": "required_fields_present",
                    "fields": [
                        "survey_time",
                        "source_activity_id",
                        "raw_pairs",
                    ],
                    "scope": "all_records",
                },
                {
                    "type": "field_types",
                    "fields": {
                        "survey_time": "datetime-string",
                        "source_activity_id": "string",
                        "gender": "string-or-null",
                        "age": "integer-or-null",
                    },
                    "scope": "all_records",
                },
            ],
            "tags": ["public", "fhir", "interop", "survey", "gamebus-shaped"],
        },
        {
            "task_id": "PUB_103",
            "title": "Map FHIR Observation daily activity summary to day_aggregate",
            "difficulty": "medium",
            "source_family": "fhir_activity_summary_observation",
            "target_entity": "daily_activity_summary",
            "task_text": (
                "Create one GameBus-like daily activity summary from one FHIR Observation with activity components."
            ),
            "source_schema": _obj(
                ["resourceType", "id", "effectiveDateTime", "component"],
                {
                    "resourceType": {"type": "string", "const": "Observation"},
                    "id": {"type": "string"},
                    "effectiveDateTime": {"type": "string"},
                    "component": {
                        "type": "array",
                        "items": _obj(
                            ["code"],
                            {
                                "code": _obj([], {"text": {"type": "string"}}),
                                "valueInteger": {"type": "integer"},
                                "valueDecimal": {"type": "number"},
                            },
                        ),
                    },
                },
                title="FHIRActivitySummaryObservation",
            ),
            "target_schema": _obj(
                ["source_activity_id", "date", "steps_sum", "distance_sum_m", "pa_cal_sum_kcal"],
                {
                    "source_activity_id": {"type": "string"},
                    "date": {"type": "string"},
                    "steps_sum": {"type": "integer"},
                    "distance_sum_m": {"type": "number"},
                    "pa_cal_sum_kcal": {"type": "integer"},
                },
                title="DailyActivitySummary",
            ),
            "input_fixture": {
                "records": [
                    {
                        "resourceType": "Observation",
                        "id": "obs-day-001",
                        "effectiveDateTime": "2026-01-16T22:00:00Z",
                        "component": [
                            {"code": {"text": "steps_sum"}, "valueInteger": 1384},
                            {"code": {"text": "distance_sum_m"}, "valueDecimal": 1028.328125},
                            {"code": {"text": "pa_cal_sum_kcal"}, "valueInteger": 50},
                        ],
                    }
                ]
            },
            "expected_fixture": {
                "records": [
                    {
                        "source_activity_id": "obs-day-001",
                        "date": "2026-01-16",
                        "steps_sum": 1384,
                        "distance_sum_m": 1028.328125,
                        "pa_cal_sum_kcal": 50,
                    }
                ]
            },
            "matches": [
                {"target_field": "source_activity_id", "source_path": "$.id", "relation": "exact"},
                {"target_field": "date", "source_path": "$.effectiveDateTime", "relation": "transform:truncate_date"},
                {"target_field": "steps_sum", "source_path": "$.component[*]", "relation": "transform:extract_array_field(code.text=steps_sum)"},
                {"target_field": "distance_sum_m", "source_path": "$.component[*]", "relation": "transform:extract_array_field(code.text=distance_sum_m)"},
                {"target_field": "pa_cal_sum_kcal", "source_path": "$.component[*]", "relation": "transform:extract_array_field(code.text=pa_cal_sum_kcal)"},
            ],
            "plan": {
                "plan_id": "PUB_103_gold",
                "task_id": "PUB_103",
                "target_entity": "daily_activity_summary",
                "field_mappings": [
                    {"target_field": "source_activity_id", "operation": "copy", "source_paths": ["$.id"]},
                    {"target_field": "date", "operation": "truncate_date", "source_paths": ["$.effectiveDateTime"]},
                    {
                        "target_field": "steps_sum",
                        "operation": "extract_array_field",
                        "source_paths": ["$.component"],
                        "parameters": {"match_field": "code.text", "match_value": "steps_sum", "value_field": "valueInteger"},
                    },
                    {
                        "target_field": "distance_sum_m",
                        "operation": "extract_array_field",
                        "source_paths": ["$.component"],
                        "parameters": {"match_field": "code.text", "match_value": "distance_sum_m", "value_field": "valueDecimal"},
                    },
                    {
                        "target_field": "pa_cal_sum_kcal",
                        "operation": "extract_array_field",
                        "source_paths": ["$.component"],
                        "parameters": {"match_field": "code.text", "match_value": "pa_cal_sum_kcal", "value_field": "valueInteger"},
                    },
                ],
                "joins": [],
                "filters": [],
                "aggregations": [],
                "assumptions": [],
                "confidence": 1.0,
            },
            "invariants": [
                {"type": "range", "field": "steps_sum", "min": 0},
                {"type": "range", "field": "distance_sum_m", "min": 0},
            ],
            "downstream_checks": [
                {"type": "record_count_matches_expected"},
                {
                    "type": "required_fields_present",
                    "fields": [
                        "source_activity_id",
                        "date",
                        "steps_sum",
                        "distance_sum_m",
                        "pa_cal_sum_kcal",
                    ],
                    "scope": "all_records",
                },
                {
                    "type": "field_types",
                    "fields": {
                        "source_activity_id": "string",
                        "date": "date-string",
                        "steps_sum": "integer",
                        "distance_sum_m": "number",
                        "pa_cal_sum_kcal": "integer",
                    },
                    "scope": "all_records",
                },
            ],
            "tags": ["public", "fhir", "interop", "activity", "gamebus-shaped"],
        },
        {
            "task_id": "PUB_104",
            "title": "Map FHIR Observation nutrition summary to daily_nutrition_summary",
            "difficulty": "medium",
            "source_family": "fhir_nutrition_summary_observation",
            "target_entity": "daily_nutrition_summary",
            "task_text": (
                "Create one GameBus-like daily nutrition summary from one FHIR Observation with nutrition components."
            ),
            "source_schema": _obj(
                ["resourceType", "id", "effectiveDateTime", "component"],
                {
                    "resourceType": {"type": "string", "const": "Observation"},
                    "id": {"type": "string"},
                    "effectiveDateTime": {"type": "string"},
                    "component": {
                        "type": "array",
                        "items": _obj(
                            ["code"],
                            {
                                "code": _obj([], {"text": {"type": "string"}}),
                                "valueInteger": {"type": "integer"},
                                "valueString": {"type": "string"},
                            },
                        ),
                    },
                },
                title="FHIRNutritionSummaryObservation",
            ),
            "target_schema": _obj(
                ["source_activity_id", "date", "aggregation_level", "summary_score", "carbs_conformance", "fat_conformance", "fiber_conformance"],
                {
                    "source_activity_id": {"type": "string"},
                    "date": {"type": "string"},
                    "aggregation_level": {"type": "string"},
                    "summary_score": {"type": "integer"},
                    "carbs_conformance": {"type": "integer"},
                    "fat_conformance": {"type": "integer"},
                    "fiber_conformance": {"type": "integer"},
                },
                title="DailyNutritionSummary",
            ),
            "input_fixture": {
                "records": [
                    {
                        "resourceType": "Observation",
                        "id": "obs-nutri-001",
                        "effectiveDateTime": "2026-01-16T22:00:00Z",
                        "component": [
                            {"code": {"text": "aggregation_level"}, "valueString": "DAILY"},
                            {"code": {"text": "summary_score"}, "valueInteger": 78},
                            {"code": {"text": "carbs_conformance"}, "valueInteger": 80},
                            {"code": {"text": "fat_conformance"}, "valueInteger": 76},
                            {"code": {"text": "fiber_conformance"}, "valueInteger": 65},
                        ],
                    }
                ]
            },
            "expected_fixture": {
                "records": [
                    {
                        "source_activity_id": "obs-nutri-001",
                        "date": "2026-01-16",
                        "aggregation_level": "DAILY",
                        "summary_score": 78,
                        "carbs_conformance": 80,
                        "fat_conformance": 76,
                        "fiber_conformance": 65,
                    }
                ]
            },
            "matches": [
                {"target_field": "source_activity_id", "source_path": "$.id", "relation": "exact"},
                {"target_field": "date", "source_path": "$.effectiveDateTime", "relation": "transform:truncate_date"},
                {"target_field": "aggregation_level", "source_path": "$.component[*]", "relation": "transform:extract_array_field(code.text=aggregation_level)"},
                {"target_field": "summary_score", "source_path": "$.component[*]", "relation": "transform:extract_array_field(code.text=summary_score)"},
                {"target_field": "carbs_conformance", "source_path": "$.component[*]", "relation": "transform:extract_array_field(code.text=carbs_conformance)"},
                {"target_field": "fat_conformance", "source_path": "$.component[*]", "relation": "transform:extract_array_field(code.text=fat_conformance)"},
                {"target_field": "fiber_conformance", "source_path": "$.component[*]", "relation": "transform:extract_array_field(code.text=fiber_conformance)"},
            ],
            "plan": {
                "plan_id": "PUB_104_gold",
                "task_id": "PUB_104",
                "target_entity": "daily_nutrition_summary",
                "field_mappings": [
                    {"target_field": "source_activity_id", "operation": "copy", "source_paths": ["$.id"]},
                    {"target_field": "date", "operation": "truncate_date", "source_paths": ["$.effectiveDateTime"]},
                    {
                        "target_field": "aggregation_level",
                        "operation": "extract_array_field",
                        "source_paths": ["$.component"],
                        "parameters": {"match_field": "code.text", "match_value": "aggregation_level", "value_field": "valueString"},
                    },
                    {
                        "target_field": "summary_score",
                        "operation": "extract_array_field",
                        "source_paths": ["$.component"],
                        "parameters": {"match_field": "code.text", "match_value": "summary_score", "value_field": "valueInteger"},
                    },
                    {
                        "target_field": "carbs_conformance",
                        "operation": "extract_array_field",
                        "source_paths": ["$.component"],
                        "parameters": {"match_field": "code.text", "match_value": "carbs_conformance", "value_field": "valueInteger"},
                    },
                    {
                        "target_field": "fat_conformance",
                        "operation": "extract_array_field",
                        "source_paths": ["$.component"],
                        "parameters": {"match_field": "code.text", "match_value": "fat_conformance", "value_field": "valueInteger"},
                    },
                    {
                        "target_field": "fiber_conformance",
                        "operation": "extract_array_field",
                        "source_paths": ["$.component"],
                        "parameters": {"match_field": "code.text", "match_value": "fiber_conformance", "value_field": "valueInteger"},
                    },
                ],
                "joins": [],
                "filters": [],
                "aggregations": [],
                "assumptions": [],
                "confidence": 1.0,
            },
            "invariants": [
                {"type": "range", "field": "summary_score", "min": 0},
                {"type": "field_type", "field": "aggregation_level", "expect": "string"},
            ],
            "downstream_checks": [
                {"type": "record_count_matches_expected"},
                {
                    "type": "required_fields_present",
                    "fields": [
                        "source_activity_id",
                        "date",
                        "aggregation_level",
                        "summary_score",
                        "carbs_conformance",
                        "fat_conformance",
                        "fiber_conformance",
                    ],
                    "scope": "all_records",
                },
                {
                    "type": "field_types",
                    "fields": {
                        "source_activity_id": "string",
                        "date": "date-string",
                        "aggregation_level": "string",
                        "summary_score": "integer",
                        "carbs_conformance": "integer",
                        "fat_conformance": "integer",
                        "fiber_conformance": "integer",
                    },
                    "scope": "all_records",
                },
            ],
            "tags": ["public", "fhir", "interop", "nutrition", "gamebus-shaped"],
        },
    ]