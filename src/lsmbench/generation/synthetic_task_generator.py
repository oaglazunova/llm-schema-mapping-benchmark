from __future__ import annotations

from typing import Any


def _obj_schema(
    fields: list[tuple[str, dict[str, Any]]],
    *,
    title: str,
    required: list[str] | None = None,
) -> dict[str, Any]:
    props = {name: spec for name, spec in fields}
    return {
        "type": "object",
        "title": title,
        "properties": props,
        "required": required or [name for name, _ in fields],
        "additionalProperties": False,
    }


def _task_spec(
    *,
    task_id: str,
    title: str,
    difficulty: str,
    source_family: str,
    target_entity: str,
    task_text: str,
    source_schema: dict[str, Any],
    target_schema: dict[str, Any],
    input_fixture: dict[str, Any],
    expected_fixture: dict[str, Any],
    matches: list[dict[str, Any]],
    plan: dict[str, Any],
    invariants: list[dict[str, Any]],
    tags: list[str],
    downstream_checks: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "task_id": task_id,
        "title": title,
        "difficulty": difficulty,
        "source_family": source_family,
        "target_entity": target_entity,
        "task_text": task_text,
        "source_schema": source_schema,
        "target_schema": target_schema,
        "input_fixture": input_fixture,
        "expected_fixture": expected_fixture,
        "matches": matches,
        "plan": plan,
        "invariants": invariants,
        "tags": tags,
        "downstream_checks": downstream_checks or [],
    }


def native_synthetic_specs() -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []

    # ------------------------------------------------------------------
    # SYN_001 - flat daily nutrient-style summary
    # ------------------------------------------------------------------
    source_schema = _obj_schema(
        [
            ("EVENT_TS", {"type": "string"}),
            ("EVENT_ID", {"type": "integer"}),
            ("LEVEL", {"type": "string"}),
            ("TOTAL_SCORE", {"type": "string"}),
            ("PROTEIN_SCORE", {"type": "string"}),
            ("SUGAR_SCORE", {"type": "string"}),
        ],
        title="SyntheticDailyNutrientRecord",
    )

    target_schema = _obj_schema(
        [
            ("source_event_id", {"type": "integer"}),
            ("date", {"type": "string"}),
            ("level", {"type": "string"}),
            ("total_score", {"type": "integer"}),
            ("protein_score", {"type": "integer"}),
            ("sugar_score", {"type": "integer"}),
        ],
        title="SyntheticDailyNutrientSummary",
    )

    input_fixture = {
        "records": [
            {
                "EVENT_TS": "2025-10-05 08:30:00",
                "EVENT_ID": 1001,
                "LEVEL": "DAILY",
                "TOTAL_SCORE": "82",
                "PROTEIN_SCORE": "74",
                "SUGAR_SCORE": "91",
            }
        ]
    }
    expected_fixture = {
        "records": [
            {
                "source_event_id": 1001,
                "date": "2025-10-05",
                "level": "DAILY",
                "total_score": 82,
                "protein_score": 74,
                "sugar_score": 91,
            }
        ]
    }
    specs.append(
        _task_spec(
            task_id="SYN_001",
            title="Map daily nutrient metrics to nutrient summary",
            difficulty="easy",
            source_family="synthetic_daily_nutrient_metrics",
            target_entity="synthetic_daily_nutrient_summary",
            task_text="Map one flat source record into one canonical daily nutrient summary object.",
            source_schema=source_schema,
            target_schema=target_schema,
            input_fixture=input_fixture,
            expected_fixture=expected_fixture,
            matches=[
                {"target_field": "source_event_id", "source_path": "$.EVENT_ID", "relation": "exact"},
                {"target_field": "date", "source_path": "$.EVENT_TS", "relation": "transform:truncate_date"},
                {"target_field": "level", "source_path": "$.LEVEL", "relation": "exact"},
                {"target_field": "total_score", "source_path": "$.TOTAL_SCORE", "relation": "transform:cast_integer"},
                {"target_field": "protein_score", "source_path": "$.PROTEIN_SCORE", "relation": "transform:cast_integer"},
                {"target_field": "sugar_score", "source_path": "$.SUGAR_SCORE", "relation": "transform:cast_integer"},
            ],
            plan={
                "plan_id": "SYN_001_gold",
                "task_id": "SYN_001",
                "target_entity": "synthetic_daily_nutrient_summary",
                "field_mappings": [
                    {"target_field": "source_event_id", "operation": "copy", "source_paths": ["$.EVENT_ID"]},
                    {"target_field": "date", "operation": "truncate_date", "source_paths": ["$.EVENT_TS"]},
                    {"target_field": "level", "operation": "copy", "source_paths": ["$.LEVEL"]},
                    {"target_field": "total_score", "operation": "cast_integer", "source_paths": ["$.TOTAL_SCORE"]},
                    {"target_field": "protein_score", "operation": "cast_integer", "source_paths": ["$.PROTEIN_SCORE"]},
                    {"target_field": "sugar_score", "operation": "cast_integer", "source_paths": ["$.SUGAR_SCORE"]},
                ],
                "joins": [],
                "filters": [],
                "aggregations": [],
                "assumptions": ["One source record becomes one target record."],
                "confidence": 1.0,
            },
            invariants=[
                {"type": "field_type", "field": "date", "expect": "date-string"},
                {"type": "field_type", "field": "total_score", "expect": "integer"},
            ],
            tags=["synthetic", "flat", "casting", "date-normalization"],
        )
    )

    # ------------------------------------------------------------------
    # SYN_002 - flat daily activity-style summary
    # ------------------------------------------------------------------
    source_schema = _obj_schema(
        [
            ("TS", {"type": "string"}),
            ("ROW_ID", {"type": "integer"}),
            ("STEP_TOTAL", {"type": "string"}),
            ("DIST_M", {"type": "string"}),
            ("ACTIVE_KCAL", {"type": "string"}),
        ],
        title="SyntheticDailyActivityAggregate",
    )
    target_schema = _obj_schema(
        [
            ("source_row_id", {"type": "integer"}),
            ("date", {"type": "string"}),
            ("steps_sum", {"type": "integer"}),
            ("distance_sum_m", {"type": "number"}),
            ("active_kcal_sum", {"type": "integer"}),
        ],
        title="SyntheticDailyActivitySummary",
    )
    input_fixture = {
        "records": [
            {
                "TS": "2025-10-03 23:59:59",
                "ROW_ID": 2001,
                "STEP_TOTAL": "6388",
                "DIST_M": "4820.5",
                "ACTIVE_KCAL": "248",
            }
        ]
    }
    expected_fixture = {
        "records": [
            {
                "source_row_id": 2001,
                "date": "2025-10-03",
                "steps_sum": 6388,
                "distance_sum_m": 4820.5,
                "active_kcal_sum": 248,
            }
        ]
    }
    specs.append(
        _task_spec(
            task_id="SYN_002",
            title="Map daily activity aggregate to activity summary",
            difficulty="easy",
            source_family="synthetic_daily_activity_aggregate",
            target_entity="synthetic_daily_activity_summary",
            task_text="Map one daily aggregate record into one activity summary object.",
            source_schema=source_schema,
            target_schema=target_schema,
            input_fixture=input_fixture,
            expected_fixture=expected_fixture,
            matches=[
                {"target_field": "source_row_id", "source_path": "$.ROW_ID", "relation": "exact"},
                {"target_field": "date", "source_path": "$.TS", "relation": "transform:truncate_date"},
                {"target_field": "steps_sum", "source_path": "$.STEP_TOTAL", "relation": "transform:cast_integer"},
                {"target_field": "distance_sum_m", "source_path": "$.DIST_M", "relation": "transform:cast_number"},
                {"target_field": "active_kcal_sum", "source_path": "$.ACTIVE_KCAL", "relation": "transform:cast_integer"},
            ],
            plan={
                "plan_id": "SYN_002_gold",
                "task_id": "SYN_002",
                "target_entity": "synthetic_daily_activity_summary",
                "field_mappings": [
                    {"target_field": "source_row_id", "operation": "copy", "source_paths": ["$.ROW_ID"]},
                    {"target_field": "date", "operation": "truncate_date", "source_paths": ["$.TS"]},
                    {"target_field": "steps_sum", "operation": "cast_integer", "source_paths": ["$.STEP_TOTAL"]},
                    {"target_field": "distance_sum_m", "operation": "cast_number", "source_paths": ["$.DIST_M"]},
                    {"target_field": "active_kcal_sum", "operation": "cast_integer", "source_paths": ["$.ACTIVE_KCAL"]},
                ],
                "joins": [],
                "filters": [],
                "aggregations": [],
                "assumptions": ["Source values are already aggregated."],
                "confidence": 1.0,
            },
            invariants=[
                {"type": "range", "field": "steps_sum", "min": 0},
                {"type": "range", "field": "distance_sum_m", "min": 0},
            ],
            tags=["synthetic", "flat", "numeric-summary"],
        )
    )

    # ------------------------------------------------------------------
    # SYN_003 - app route event
    # ------------------------------------------------------------------
    source_schema = _obj_schema(
        [
            ("TS", {"type": "string"}),
            ("EVENT_ID", {"type": "integer"}),
            ("APP_NAME", {"type": "string"}),
            ("SESSION_KEY", {"type": "string"}),
            ("USER_TOKEN", {"type": "string"}),
            ("CAMPAIGN_CODE", {"type": "string"}),
            ("ROUTE", {"type": "string"}),
        ],
        title="SyntheticRouteEvent",
    )
    target_schema = _obj_schema(
        [
            ("event_time", {"type": "string"}),
            ("source_event_id", {"type": "integer"}),
            ("app", {"type": "string"}),
            ("session_id", {"type": "string"}),
            ("user_external_id", {"type": "string"}),
            ("campaign", {"type": "string"}),
            ("uri", {"type": "string"}),
        ],
        title="SyntheticNavigationEvent",
    )
    input_fixture = {
        "records": [
            {
                "TS": "2025-10-04 16:12:08",
                "EVENT_ID": 3001,
                "APP_NAME": "example.app",
                "SESSION_KEY": "sess-1",
                "USER_TOKEN": "u-42",
                "CAMPAIGN_CODE": "camp-a",
                "ROUTE": "/account",
            }
        ]
    }
    expected_fixture = {
        "records": [
            {
                "event_time": "2025-10-04 16:12:08",
                "source_event_id": 3001,
                "app": "example.app",
                "session_id": "sess-1",
                "user_external_id": "u-42",
                "campaign": "camp-a",
                "uri": "/account",
            }
        ]
    }
    specs.append(
        _task_spec(
            task_id="SYN_003",
            title="Map route event to navigation event",
            difficulty="easy",
            source_family="synthetic_route_event",
            target_entity="synthetic_navigation_event",
            task_text="Map one app route event to one canonical navigation event object.",
            source_schema=source_schema,
            target_schema=target_schema,
            input_fixture=input_fixture,
            expected_fixture=expected_fixture,
            matches=[
                {"target_field": "event_time", "source_path": "$.TS", "relation": "transform:parse_datetime"},
                {"target_field": "source_event_id", "source_path": "$.EVENT_ID", "relation": "exact"},
                {"target_field": "app", "source_path": "$.APP_NAME", "relation": "exact"},
                {"target_field": "session_id", "source_path": "$.SESSION_KEY", "relation": "exact"},
                {"target_field": "user_external_id", "source_path": "$.USER_TOKEN", "relation": "exact"},
                {"target_field": "campaign", "source_path": "$.CAMPAIGN_CODE", "relation": "exact"},
                {"target_field": "uri", "source_path": "$.ROUTE", "relation": "exact"},
            ],
            plan={
                "plan_id": "SYN_003_gold",
                "task_id": "SYN_003",
                "target_entity": "synthetic_navigation_event",
                "field_mappings": [
                    {"target_field": "event_time", "operation": "parse_datetime", "source_paths": ["$.TS"]},
                    {"target_field": "source_event_id", "operation": "copy", "source_paths": ["$.EVENT_ID"]},
                    {"target_field": "app", "operation": "copy", "source_paths": ["$.APP_NAME"]},
                    {"target_field": "session_id", "operation": "copy", "source_paths": ["$.SESSION_KEY"]},
                    {"target_field": "user_external_id", "operation": "copy", "source_paths": ["$.USER_TOKEN"]},
                    {"target_field": "campaign", "operation": "copy", "source_paths": ["$.CAMPAIGN_CODE"]},
                    {"target_field": "uri", "operation": "copy", "source_paths": ["$.ROUTE"]},
                ],
                "joins": [],
                "filters": [],
                "aggregations": [],
                "assumptions": ["One source record becomes one target event."],
                "confidence": 1.0,
            },
            invariants=[
                {"type": "prefix", "field": "uri", "value": "/"},
                {"type": "non_empty", "field": "session_id"},
            ],
            tags=["synthetic", "event", "session-metadata"],
        )
    )

    # ------------------------------------------------------------------
    # SYN_004 - embedded JSON consent
    # ------------------------------------------------------------------
    source_schema = _obj_schema(
        [
            ("TS", {"type": "string"}),
            ("EVENT_ID", {"type": "integer"}),
            ("CAMPAIGN_CODE", {"type": "string"}),
            ("CONSENT_PAYLOAD", {"type": "string"}),
        ],
        title="SyntheticConsentPayloadRecord",
    )
    target_schema = _obj_schema(
        [
            ("consent_time", {"type": "string"}),
            ("source_event_id", {"type": "integer"}),
            ("campaign", {"type": "string"}),
            ("consent_items", {"type": "array"}),
            ("accepted_count", {"type": "integer"}),
            ("item_count", {"type": "integer"}),
            ("all_required_accepted", {"type": "boolean"}),
        ],
        title="SyntheticConsentRecord",
    )
    input_fixture = {
        "records": [
            {
                "TS": "2025-04-11 16:32:37",
                "EVENT_ID": 4001,
                "CAMPAIGN_CODE": "camp-a",
                "CONSENT_PAYLOAD": '[{"code":"data","accepted":true},{"code":"terms","accepted":true},{"code":"reminders","accepted":false}]',
            }
        ]
    }
    expected_fixture = {
        "records": [
            {
                "consent_time": "2025-04-11 16:32:37",
                "source_event_id": 4001,
                "campaign": "camp-a",
                "consent_items": [
                    {"code": "data", "accepted": True},
                    {"code": "terms", "accepted": True},
                    {"code": "reminders", "accepted": False},
                ],
                "accepted_count": 2,
                "item_count": 3,
                "all_required_accepted": False,
            }
        ]
    }
    specs.append(
        _task_spec(
            task_id="SYN_004",
            title="Map embedded JSON consent payload to consent record",
            difficulty="medium",
            source_family="synthetic_embedded_json_consent",
            target_entity="synthetic_consent_record",
            task_text="Parse a JSON string and derive consent summary fields.",
            source_schema=source_schema,
            target_schema=target_schema,
            input_fixture=input_fixture,
            expected_fixture=expected_fixture,
            matches=[
                {"target_field": "consent_time", "source_path": "$.TS", "relation": "transform:parse_datetime"},
                {"target_field": "source_event_id", "source_path": "$.EVENT_ID", "relation": "exact"},
                {"target_field": "campaign", "source_path": "$.CAMPAIGN_CODE", "relation": "exact"},
                {"target_field": "consent_items", "source_path": "$.CONSENT_PAYLOAD", "relation": "transform:parse_json_array"},
            ],
            plan={
                "plan_id": "SYN_004_gold",
                "task_id": "SYN_004",
                "target_entity": "synthetic_consent_record",
                "field_mappings": [
                    {"target_field": "consent_time", "operation": "parse_datetime", "source_paths": ["$.TS"]},
                    {"target_field": "source_event_id", "operation": "copy", "source_paths": ["$.EVENT_ID"]},
                    {"target_field": "campaign", "operation": "copy", "source_paths": ["$.CAMPAIGN_CODE"]},
                    {"target_field": "consent_items", "operation": "parse_json_array", "source_paths": ["$.CONSENT_PAYLOAD"]},
                ],
                "joins": [],
                "filters": [],
                "aggregations": [
                    {"target_field": "accepted_count", "function": "count_true", "source_path": "$.consent_items[*].accepted"},
                    {"target_field": "item_count", "function": "count", "source_path": "$.consent_items[*]"},
                    {"target_field": "all_required_accepted", "function": "all_true", "source_path": "$.consent_items[*].accepted"},
                ],
                "assumptions": ["CONSENT_PAYLOAD is a valid JSON string."],
                "confidence": 1.0,
            },
            invariants=[
                {"type": "formula", "expr": "accepted_count <= item_count"},
                {"type": "field_type", "field": "all_required_accepted", "expect": "boolean"},
            ],
            tags=["synthetic", "embedded-json", "boolean-aggregation"],
        )
    )

    # ------------------------------------------------------------------
    # SYN_005 - key:value extraction
    # ------------------------------------------------------------------
    source_schema = _obj_schema(
        [
            ("TS", {"type": "string"}),
            ("EVENT_ID", {"type": "integer"}),
            ("FACTS", {"type": "array", "items": {"type": "string"}}),
        ],
        title="SyntheticProfileFactsRecord",
    )
    target_schema = _obj_schema(
        [
            ("survey_time", {"type": "string"}),
            ("source_event_id", {"type": "integer"}),
            ("gender", {"type": ["string", "null"]}),
            ("age", {"type": ["integer", "null"]}),
            ("raw_pairs", {"type": "array", "items": {"type": "string"}}),
        ],
        title="SyntheticProfileAttributes",
    )
    input_fixture = {
        "records": [
            {
                "TS": "2025-05-28 23:42:53",
                "EVENT_ID": 5001,
                "FACTS": ["gender:female", "age:23"],
            }
        ]
    }
    expected_fixture = {
        "records": [
            {
                "survey_time": "2025-05-28 23:42:53",
                "source_event_id": 5001,
                "gender": "female",
                "age": 23,
                "raw_pairs": ["gender:female", "age:23"],
            }
        ]
    }
    specs.append(
        _task_spec(
            task_id="SYN_005",
            title="Map key:value facts to profile attributes",
            difficulty="medium",
            source_family="synthetic_key_value_profile",
            target_entity="synthetic_profile_attributes",
            task_text="Extract typed attributes from key:value strings.",
            source_schema=source_schema,
            target_schema=target_schema,
            input_fixture=input_fixture,
            expected_fixture=expected_fixture,
            matches=[
                {"target_field": "survey_time", "source_path": "$.TS", "relation": "transform:parse_datetime"},
                {"target_field": "source_event_id", "source_path": "$.EVENT_ID", "relation": "exact"},
                {"target_field": "raw_pairs", "source_path": "$.FACTS", "relation": "exact"},
                {"target_field": "gender", "source_path": "$.FACTS", "relation": "transform:extract_kv_value(key=gender)"},
                {"target_field": "age", "source_path": "$.FACTS", "relation": "transform:extract_kv_value_cast_integer(key=age)"},
            ],
            plan={
                "plan_id": "SYN_005_gold",
                "task_id": "SYN_005",
                "target_entity": "synthetic_profile_attributes",
                "field_mappings": [
                    {"target_field": "survey_time", "operation": "parse_datetime", "source_paths": ["$.TS"]},
                    {"target_field": "source_event_id", "operation": "copy", "source_paths": ["$.EVENT_ID"]},
                    {"target_field": "raw_pairs", "operation": "copy", "source_paths": ["$.FACTS"]},
                    {
                        "target_field": "gender",
                        "operation": "extract_kv_value",
                        "source_paths": ["$.FACTS"],
                        "parameters": {"key": "gender", "delimiter": ":"},
                    },
                    {
                        "target_field": "age",
                        "operation": "extract_kv_value_cast_integer",
                        "source_paths": ["$.FACTS"],
                        "parameters": {"key": "age", "delimiter": ":"},
                    },
                ],
                "joins": [],
                "filters": [],
                "aggregations": [],
                "assumptions": ["FACTS is a list of key:value strings."],
                "confidence": 1.0,
            },
            invariants=[
                {"type": "field_type", "field": "age", "expect": "integer-or-null"},
                {"type": "field_type", "field": "gender", "expect": "string-or-null"},
            ],
            tags=["synthetic", "key-value", "semi-structured"],
        )
    )

    # ------------------------------------------------------------------
    # SYN_006 - grouped customer payment summary
    # ------------------------------------------------------------------
    source_schema = _obj_schema(
        [
            ("event_id", {"type": "string"}),
            ("customer_id", {"type": "string"}),
            ("currency", {"type": "string"}),
            ("created_at", {"type": "string"}),
            ("amount_minor", {"type": "integer"}),
        ],
        title="SyntheticPaymentEvent",
    )
    target_schema = _obj_schema(
        [
            ("customer_id", {"type": "string"}),
            ("currency", {"type": "string"}),
            ("payment_count", {"type": "integer"}),
            ("total_amount_minor", {"type": "number"}),
            ("first_payment_time", {"type": "string"}),
            ("last_payment_time", {"type": "string"}),
        ],
        title="SyntheticCustomerPaymentSummary",
    )
    input_fixture = {
        "records": [
            {"event_id": "e1", "customer_id": "c1", "currency": "EUR", "created_at": "2025-09-01 10:00:00", "amount_minor": 1000},
            {"event_id": "e2", "customer_id": "c1", "currency": "EUR", "created_at": "2025-09-01 11:00:00", "amount_minor": 2500},
            {"event_id": "e3", "customer_id": "c2", "currency": "USD", "created_at": "2025-09-02 09:00:00", "amount_minor": 500},
        ]
    }
    expected_fixture = {
        "records": [
            {"customer_id": "c1", "currency": "EUR", "payment_count": 2, "total_amount_minor": 3500, "first_payment_time": "2025-09-01 10:00:00", "last_payment_time": "2025-09-01 11:00:00"},
            {"customer_id": "c2", "currency": "USD", "payment_count": 1, "total_amount_minor": 500, "first_payment_time": "2025-09-02 09:00:00", "last_payment_time": "2025-09-02 09:00:00"},
        ]
    }
    specs.append(
        _task_spec(
            task_id="SYN_006",
            title="Group payment events into customer payment summaries",
            difficulty="medium",
            source_family="synthetic_payment_event",
            target_entity="synthetic_customer_payment_summary",
            task_text="Group payment events by customer and compute summary fields.",
            source_schema=source_schema,
            target_schema=target_schema,
            input_fixture=input_fixture,
            expected_fixture=expected_fixture,
            matches=[
                {"target_field": "customer_id", "source_path": "$.customer_id", "relation": "group-key"},
                {"target_field": "currency", "source_path": "$.currency", "relation": "exact:first-in-group"},
                {"target_field": "payment_count", "source_path": "$.event_id", "relation": "aggregate:count"},
                {"target_field": "total_amount_minor", "source_path": "$.amount_minor", "relation": "aggregate:sum"},
                {"target_field": "first_payment_time", "source_path": "$.created_at", "relation": "aggregate:first"},
                {"target_field": "last_payment_time", "source_path": "$.created_at", "relation": "aggregate:latest"},
            ],
            plan={
                "plan_id": "SYN_006_gold",
                "task_id": "SYN_006",
                "target_entity": "synthetic_customer_payment_summary",
                "group_by_paths": ["$.customer_id"],
                "field_mappings": [
                    {"target_field": "customer_id", "operation": "copy", "source_paths": ["$.customer_id"]},
                    {"target_field": "currency", "operation": "copy", "source_paths": ["$.currency"]},
                ],
                "joins": [],
                "filters": [],
                "aggregations": [
                    {"target_field": "payment_count", "function": "count", "source_path": "$.event_id"},
                    {"target_field": "total_amount_minor", "function": "sum", "source_path": "$.amount_minor"},
                    {"target_field": "first_payment_time", "function": "first", "source_path": "$.created_at"},
                    {"target_field": "last_payment_time", "function": "latest", "source_path": "$.created_at"},
                ],
                "assumptions": ["Input order reflects event order within each customer group."],
                "confidence": 1.0,
            },
            invariants=[
                {"type": "record_count_matches_expected"},
                {"type": "field_type", "field": "payment_count", "expect": "integer"},
            ],
            tags=["synthetic", "grouped-execution", "aggregation"],
            downstream_checks=[
                {"type": "record_count_matches_expected"},
                {
                    "type": "required_fields_present",
                    "fields": [
                        "customer_id",
                        "currency",
                        "payment_count",
                        "total_amount_minor",
                        "first_payment_time",
                        "last_payment_time",
                    ],
                    "scope": "all_records",
                },
            ],
        )
    )

    # ------------------------------------------------------------------
    # SYN_007 - grouped session route summary
    # ------------------------------------------------------------------
    source_schema = _obj_schema(
        [
            ("event_id", {"type": "integer"}),
            ("session_id", {"type": "string"}),
            ("route", {"type": "string"}),
            ("event_time", {"type": "string"}),
        ],
        title="SyntheticSessionRouteEvent",
    )
    target_schema = _obj_schema(
        [
            ("session_id", {"type": "string"}),
            ("page_visit_count", {"type": "integer"}),
            ("first_route", {"type": "string"}),
            ("last_route", {"type": "string"}),
            ("distinct_route_count", {"type": "integer"}),
        ],
        title="SyntheticSessionRouteSummary",
    )
    input_fixture = {
        "records": [
            {"event_id": 1, "session_id": "s1", "route": "/home", "event_time": "2025-09-10 10:00:00"},
            {"event_id": 2, "session_id": "s1", "route": "/account", "event_time": "2025-09-10 10:02:00"},
            {"event_id": 3, "session_id": "s1", "route": "/account", "event_time": "2025-09-10 10:03:00"},
        ]
    }
    expected_fixture = {
        "records": [
            {"session_id": "s1", "page_visit_count": 3, "first_route": "/home", "last_route": "/account", "distinct_route_count": 2}
        ]
    }
    specs.append(
        _task_spec(
            task_id="SYN_007",
            title="Group session route events into a route summary",
            difficulty="hard",
            source_family="synthetic_session_route_event",
            target_entity="synthetic_session_route_summary",
            task_text="Group route events by session and compute visit statistics.",
            source_schema=source_schema,
            target_schema=target_schema,
            input_fixture=input_fixture,
            expected_fixture=expected_fixture,
            matches=[
                {"target_field": "session_id", "source_path": "$.session_id", "relation": "group-key"},
                {"target_field": "page_visit_count", "source_path": "$.event_id", "relation": "aggregate:count"},
                {"target_field": "first_route", "source_path": "$.route", "relation": "aggregate:first"},
                {"target_field": "last_route", "source_path": "$.route", "relation": "aggregate:latest"},
                {"target_field": "distinct_route_count", "source_path": "$.route", "relation": "aggregate:distinct_count"},
            ],
            plan={
                "plan_id": "SYN_007_gold",
                "task_id": "SYN_007",
                "target_entity": "synthetic_session_route_summary",
                "group_by_paths": ["$.session_id"],
                "field_mappings": [
                    {"target_field": "session_id", "operation": "copy", "source_paths": ["$.session_id"]},
                ],
                "joins": [],
                "filters": [],
                "aggregations": [
                    {"target_field": "page_visit_count", "function": "count", "source_path": "$.event_id"},
                    {"target_field": "first_route", "function": "first", "source_path": "$.route"},
                    {"target_field": "last_route", "function": "latest", "source_path": "$.route"},
                    {"target_field": "distinct_route_count", "function": "distinct_count", "source_path": "$.route"},
                ],
                "assumptions": ["Input order reflects event order within each session."],
                "confidence": 1.0,
            },
            invariants=[
                {"type": "record_count_matches_expected"},
                {"type": "formula", "expr": "distinct_route_count <= page_visit_count"},
            ],
            tags=["synthetic", "grouped-execution", "session-summary", "hard"],
        )
    )

    # ------------------------------------------------------------------
    # SYN_008 - filter + cast
    # ------------------------------------------------------------------
    source_schema = _obj_schema(
        [
            ("record_id", {"type": "integer"}),
            ("status", {"type": "string"}),
            ("score", {"type": "string"}),
            ("ts", {"type": "string"}),
        ],
        title="SyntheticFilteredScoreRecord",
    )
    target_schema = _obj_schema(
        [
            ("record_id", {"type": "integer"}),
            ("score", {"type": "integer"}),
            ("date", {"type": "string"}),
        ],
        title="SyntheticFilteredScore",
    )
    input_fixture = {
        "records": [
            {"record_id": 1, "status": "active", "score": "8", "ts": "2025-10-01 08:00:00"},
            {"record_id": 2, "status": "archived", "score": "99", "ts": "2025-10-01 08:10:00"},
        ]
    }
    expected_fixture = {
        "records": [
            {"record_id": 1, "score": 8, "date": "2025-10-01"}
        ]
    }
    specs.append(
        _task_spec(
            task_id="SYN_008",
            title="Filter active records and normalize score",
            difficulty="medium",
            source_family="synthetic_filtered_score_record",
            target_entity="synthetic_filtered_score",
            task_text="Keep only active records and normalize score/date fields.",
            source_schema=source_schema,
            target_schema=target_schema,
            input_fixture=input_fixture,
            expected_fixture=expected_fixture,
            matches=[
                {"target_field": "record_id", "source_path": "$.record_id", "relation": "exact"},
                {"target_field": "score", "source_path": "$.score", "relation": "transform:cast_integer"},
                {"target_field": "date", "source_path": "$.ts", "relation": "transform:truncate_date"},
            ],
            plan={
                "plan_id": "SYN_008_gold",
                "task_id": "SYN_008",
                "target_entity": "synthetic_filtered_score",
                "field_mappings": [
                    {"target_field": "record_id", "operation": "copy", "source_paths": ["$.record_id"]},
                    {"target_field": "score", "operation": "cast_integer", "source_paths": ["$.score"]},
                    {"target_field": "date", "operation": "truncate_date", "source_paths": ["$.ts"]},
                ],
                "joins": [],
                "filters": [
                    {"path": "$.status", "operator": "eq", "value": "active"},
                ],
                "aggregations": [],
                "assumptions": ["Only active records should be included."],
                "confidence": 1.0,
            },
            invariants=[
                {"type": "record_count_matches_expected"},
                {"type": "field_type", "field": "score", "expect": "integer"},
            ],
            tags=["synthetic", "filtering", "casting"],
        )
    )

    return specs