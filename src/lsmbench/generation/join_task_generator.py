from __future__ import annotations

from typing import Any


def _obj_schema(fields: list[tuple[str, dict[str, Any]]], *, title: str) -> dict[str, Any]:
    return {
        "type": "object",
        "title": title,
        "properties": {name: spec for name, spec in fields},
        "required": [name for name, _ in fields],
        "additionalProperties": False,
    }


def _array_of_objects(fields: list[tuple[str, dict[str, Any]]], *, title: str) -> dict[str, Any]:
    return {
        "type": "array",
        "items": _obj_schema(fields, title=title),
    }


def build_join_specs() -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []

    # SYN_301 — left join orders/customers
    specs.append(
        {
            "task_id": "SYN_301",
            "title": "Join primitive: orders with customers (left join)",
            "split": "synthetic",
            "difficulty": "medium",
            "source_family": "synthetic_join_order_customer",
            "target_entity": "order_with_customer",
            "task_text": (
                "Create one canonical order_with_customer object per order by joining "
                "orders with customers on customer_id = id using a left join."
            ),
            "primitive_family": "join",
            "primitive_subtype": "left",
            "lexical_perturbation": "none",
            "ambiguity_class": "single_gold",
            "composition_depth": 1,
            "difficulty_axes": {
                "num_source_objects": 2,
                "num_target_fields": 7,
                "num_joins": 1,
                "num_aggregations": 0,
                "nesting_depth": 1,
                "has_enum_normalization": False,
                "has_time_normalization": False,
            },
            "source_schema": {
                "type": "object",
                "title": "OrderCustomerBundle",
                "properties": {
                    "orders": _array_of_objects(
                        [
                            ("id", {"type": "string"}),
                            ("customer_id", {"type": "string"}),
                            ("status", {"type": "string"}),
                            ("total_amount", {"type": "number"}),
                        ],
                        title="OrdersRow",
                    ),
                    "customers": _array_of_objects(
                        [
                            ("id", {"type": "string"}),
                            ("email", {"type": "string"}),
                            ("country", {"type": "string"}),
                            ("tier", {"type": "string"}),
                        ],
                        title="CustomersRow",
                    ),
                },
                "required": ["orders", "customers"],
                "additionalProperties": False,
            },
            "target_schema": {
                "type": "object",
                "title": "OrderWithCustomer",
                "properties": {
                    "order_id": {"type": "string"},
                    "customer_id": {"type": "string"},
                    "customer_email": {"type": ["string", "null"]},
                    "customer_country": {"type": ["string", "null"]},
                    "customer_tier": {"type": ["string", "null"]},
                    "order_status": {"type": "string"},
                    "total_amount": {"type": "number"},
                },
                "required": [
                    "order_id",
                    "customer_id",
                    "customer_email",
                    "customer_country",
                    "customer_tier",
                    "order_status",
                    "total_amount",
                ],
                "additionalProperties": False,
            },
            "input_fixture": {
                "records": [
                    {
                        "orders": [
                            {"id": "ord-001", "customer_id": "cust-1", "status": "paid", "total_amount": 24.0},
                            {"id": "ord-002", "customer_id": "cust-2", "status": "pending", "total_amount": 11.5},
                            {"id": "ord-003", "customer_id": "cust-9", "status": "paid", "total_amount": 8.0},
                        ],
                        "customers": [
                            {"id": "cust-1", "email": "a@example.org", "country": "NL", "tier": "gold"},
                            {"id": "cust-2", "email": "b@example.org", "country": "DE", "tier": "silver"},
                        ],
                    }
                ]
            },
            "expected_fixture": {
                "records": [
                    {
                        "order_id": "ord-001",
                        "customer_id": "cust-1",
                        "customer_email": "a@example.org",
                        "customer_country": "NL",
                        "customer_tier": "gold",
                        "order_status": "paid",
                        "total_amount": 24.0,
                    },
                    {
                        "order_id": "ord-002",
                        "customer_id": "cust-2",
                        "customer_email": "b@example.org",
                        "customer_country": "DE",
                        "customer_tier": "silver",
                        "order_status": "pending",
                        "total_amount": 11.5,
                    },
                    {
                        "order_id": "ord-003",
                        "customer_id": "cust-9",
                        "customer_email": None,
                        "customer_country": None,
                        "customer_tier": None,
                        "order_status": "paid",
                        "total_amount": 8.0,
                    },
                ]
            },
            "matches": [
                {"target_field": "order_id", "source_path": "$.orders.id", "relation": "copy"},
                {"target_field": "customer_id", "source_path": "$.orders.customer_id", "relation": "copy"},
                {"target_field": "customer_email", "source_path": "$.customers.email", "relation": "copy"},
                {"target_field": "customer_country", "source_path": "$.customers.country", "relation": "copy"},
                {"target_field": "customer_tier", "source_path": "$.customers.tier", "relation": "copy"},
                {"target_field": "order_status", "source_path": "$.orders.status", "relation": "copy"},
                {"target_field": "total_amount", "source_path": "$.orders.total_amount", "relation": "copy"},
            ],
            "plan": {
                "plan_id": "SYN_301_gold",
                "task_id": "SYN_301",
                "target_entity": "order_with_customer",
                "field_mappings": [
                    {"target_field": "order_id", "operation": "copy", "source_paths": ["$.orders.id"]},
                    {"target_field": "customer_id", "operation": "copy", "source_paths": ["$.orders.customer_id"]},
                    {"target_field": "customer_email", "operation": "copy", "source_paths": ["$.customers.email"]},
                    {"target_field": "customer_country", "operation": "copy", "source_paths": ["$.customers.country"]},
                    {"target_field": "customer_tier", "operation": "copy", "source_paths": ["$.customers.tier"]},
                    {"target_field": "order_status", "operation": "copy", "source_paths": ["$.orders.status"]},
                    {"target_field": "total_amount", "operation": "copy", "source_paths": ["$.orders.total_amount"]},
                ],
                "joins": [
                    {
                        "left_path": "$.orders",
                        "right_path": "$.customers",
                        "left_key": "customer_id",
                        "right_key": "id",
                        "join_type": "left",
                    }
                ],
                "filters": [],
                "aggregations": [],
                "assumptions": [
                    "Orders are joined to customers by customer_id = id.",
                    "A missing customer row should still preserve the order via left join.",
                ],
            },
            "invariants": [
                {"type": "required_fields_present", "fields": ["order_id", "customer_id", "order_status"], "scope": "all_records"}
            ],
            "tags": ["synthetic", "primitive", "join", "left", "commerce"],
            "notes": ["Synthetic join primitive.", "Bundle-shaped join task."],
            "downstream_checks": [
                {"type": "record_count_matches_expected"},
                {"type": "required_fields_present", "fields": ["order_id", "customer_id", "order_status", "total_amount"], "scope": "all_records"},
                {
                    "type": "field_types",
                    "fields": {
                        "order_id": "string",
                        "customer_id": "string",
                        "customer_email": "string-or-null",
                        "customer_country": "string-or-null",
                        "customer_tier": "string-or-null",
                        "order_status": "string",
                        "total_amount": "number",
                    },
                    "scope": "all_records",
                },
            ],
        }
    )

    # SYN_302 — inner join orders/customers
    specs.append(
        {
            "task_id": "SYN_302",
            "title": "Join primitive: orders with customers (inner join)",
            "split": "synthetic",
            "difficulty": "medium",
            "source_family": "synthetic_join_order_customer",
            "target_entity": "order_with_customer",
            "task_text": (
                "Create one canonical order_with_customer object per matched order by joining "
                "orders with customers on customer_id = id using an inner join."
            ),
            "primitive_family": "join",
            "primitive_subtype": "inner",
            "lexical_perturbation": "none",
            "ambiguity_class": "single_gold",
            "composition_depth": 1,
            "difficulty_axes": {
                "num_source_objects": 2,
                "num_target_fields": 7,
                "num_joins": 1,
                "num_aggregations": 0,
                "nesting_depth": 1,
                "has_enum_normalization": False,
                "has_time_normalization": False,
            },
            "source_schema": {
                "type": "object",
                "title": "OrderCustomerBundleInner",
                "properties": {
                    "orders": _array_of_objects(
                        [
                            ("id", {"type": "string"}),
                            ("customer_id", {"type": "string"}),
                            ("status", {"type": "string"}),
                            ("total_amount", {"type": "number"}),
                        ],
                        title="OrdersRow",
                    ),
                    "customers": _array_of_objects(
                        [
                            ("id", {"type": "string"}),
                            ("email", {"type": "string"}),
                            ("country", {"type": "string"}),
                            ("tier", {"type": "string"}),
                        ],
                        title="CustomersRow",
                    ),
                },
                "required": ["orders", "customers"],
                "additionalProperties": False,
            },
            "target_schema": {
                "type": "object",
                "title": "OrderWithCustomer",
                "properties": {
                    "order_id": {"type": "string"},
                    "customer_id": {"type": "string"},
                    "customer_email": {"type": "string"},
                    "customer_country": {"type": "string"},
                    "customer_tier": {"type": "string"},
                    "order_status": {"type": "string"},
                    "total_amount": {"type": "number"},
                },
                "required": [
                    "order_id",
                    "customer_id",
                    "customer_email",
                    "customer_country",
                    "customer_tier",
                    "order_status",
                    "total_amount",
                ],
                "additionalProperties": False,
            },
            "input_fixture": {
                "records": [
                    {
                        "orders": [
                            {"id": "ord-010", "customer_id": "cust-10", "status": "paid", "total_amount": 40.0},
                            {"id": "ord-011", "customer_id": "cust-99", "status": "pending", "total_amount": 5.5},
                        ],
                        "customers": [
                            {"id": "cust-10", "email": "c@example.org", "country": "FR", "tier": "gold"},
                        ],
                    }
                ]
            },
            "expected_fixture": {
                "records": [
                    {
                        "order_id": "ord-010",
                        "customer_id": "cust-10",
                        "customer_email": "c@example.org",
                        "customer_country": "FR",
                        "customer_tier": "gold",
                        "order_status": "paid",
                        "total_amount": 40.0,
                    }
                ]
            },
            "matches": [
                {"target_field": "order_id", "source_path": "$.orders.id", "relation": "copy"},
                {"target_field": "customer_id", "source_path": "$.orders.customer_id", "relation": "copy"},
                {"target_field": "customer_email", "source_path": "$.customers.email", "relation": "copy"},
                {"target_field": "customer_country", "source_path": "$.customers.country", "relation": "copy"},
                {"target_field": "customer_tier", "source_path": "$.customers.tier", "relation": "copy"},
                {"target_field": "order_status", "source_path": "$.orders.status", "relation": "copy"},
                {"target_field": "total_amount", "source_path": "$.orders.total_amount", "relation": "copy"},
            ],
            "plan": {
                "plan_id": "SYN_302_gold",
                "task_id": "SYN_302",
                "target_entity": "order_with_customer",
                "field_mappings": [
                    {"target_field": "order_id", "operation": "copy", "source_paths": ["$.orders.id"]},
                    {"target_field": "customer_id", "operation": "copy", "source_paths": ["$.orders.customer_id"]},
                    {"target_field": "customer_email", "operation": "copy", "source_paths": ["$.customers.email"]},
                    {"target_field": "customer_country", "operation": "copy", "source_paths": ["$.customers.country"]},
                    {"target_field": "customer_tier", "operation": "copy", "source_paths": ["$.customers.tier"]},
                    {"target_field": "order_status", "operation": "copy", "source_paths": ["$.orders.status"]},
                    {"target_field": "total_amount", "operation": "copy", "source_paths": ["$.orders.total_amount"]},
                ],
                "joins": [
                    {
                        "left_path": "$.orders",
                        "right_path": "$.customers",
                        "left_key": "customer_id",
                        "right_key": "id",
                        "join_type": "inner",
                    }
                ],
                "filters": [],
                "aggregations": [],
                "assumptions": ["Only matched orders should remain under inner join."],
            },
            "invariants": [
                {"type": "required_fields_present", "fields": ["order_id", "customer_email"], "scope": "all_records"}
            ],
            "tags": ["synthetic", "primitive", "join", "inner", "commerce"],
            "notes": ["Synthetic join primitive.", "Bundle-shaped inner join task."],
            "downstream_checks": [
                {"type": "record_count_matches_expected"},
                {"type": "required_fields_present", "fields": ["order_id", "customer_id", "customer_email"], "scope": "all_records"},
                {
                    "type": "field_types",
                    "fields": {
                        "order_id": "string",
                        "customer_id": "string",
                        "customer_email": "string",
                        "customer_country": "string",
                        "customer_tier": "string",
                        "order_status": "string",
                        "total_amount": "number",
                    },
                    "scope": "all_records",
                },
            ],
        }
    )

    # SYN_303 — left join activities/profiles
    specs.append(
        {
            "task_id": "SYN_303",
            "title": "Join primitive: activities with profiles (left join)",
            "split": "synthetic",
            "difficulty": "medium",
            "source_family": "synthetic_join_activity_profile",
            "target_entity": "activity_with_profile",
            "task_text": (
                "Create one canonical activity_with_profile object per activity by joining "
                "activities with profiles on user_id = id using a left join."
            ),
            "primitive_family": "join",
            "primitive_subtype": "left",
            "lexical_perturbation": "none",
            "ambiguity_class": "single_gold",
            "composition_depth": 1,
            "difficulty_axes": {
                "num_source_objects": 2,
                "num_target_fields": 5,
                "num_joins": 1,
                "num_aggregations": 0,
                "nesting_depth": 1,
                "has_enum_normalization": False,
                "has_time_normalization": False,
            },
            "source_schema": {
                "type": "object",
                "title": "ActivityProfileBundle",
                "properties": {
                    "activities": _array_of_objects(
                        [
                            ("event_id", {"type": "string"}),
                            ("user_id", {"type": "string"}),
                            ("activity_type", {"type": "string"}),
                            ("steps", {"type": "integer"}),
                        ],
                        title="ActivitiesRow",
                    ),
                    "profiles": _array_of_objects(
                        [
                            ("id", {"type": "string"}),
                            ("country", {"type": "string"}),
                            ("cohort", {"type": "string"}),
                        ],
                        title="ProfilesRow",
                    ),
                },
                "required": ["activities", "profiles"],
                "additionalProperties": False,
            },
            "target_schema": {
                "type": "object",
                "title": "ActivityWithProfile",
                "properties": {
                    "event_id": {"type": "string"},
                    "user_id": {"type": "string"},
                    "activity_type": {"type": "string"},
                    "steps": {"type": "integer"},
                    "profile_country": {"type": ["string", "null"]},
                },
                "required": ["event_id", "user_id", "activity_type", "steps", "profile_country"],
                "additionalProperties": False,
            },
            "input_fixture": {
                "records": [
                    {
                        "activities": [
                            {"event_id": "ev-1", "user_id": "u1", "activity_type": "walk", "steps": 5000},
                            {"event_id": "ev-2", "user_id": "u9", "activity_type": "walk", "steps": 3000},
                        ],
                        "profiles": [
                            {"id": "u1", "country": "NL", "cohort": "A"},
                        ],
                    }
                ]
            },
            "expected_fixture": {
                "records": [
                    {"event_id": "ev-1", "user_id": "u1", "activity_type": "walk", "steps": 5000, "profile_country": "NL"},
                    {"event_id": "ev-2", "user_id": "u9", "activity_type": "walk", "steps": 3000, "profile_country": None},
                ]
            },
            "matches": [
                {"target_field": "event_id", "source_path": "$.activities.event_id", "relation": "copy"},
                {"target_field": "user_id", "source_path": "$.activities.user_id", "relation": "copy"},
                {"target_field": "activity_type", "source_path": "$.activities.activity_type", "relation": "copy"},
                {"target_field": "steps", "source_path": "$.activities.steps", "relation": "copy"},
                {"target_field": "profile_country", "source_path": "$.profiles.country", "relation": "copy"},
            ],
            "plan": {
                "plan_id": "SYN_303_gold",
                "task_id": "SYN_303",
                "target_entity": "activity_with_profile",
                "field_mappings": [
                    {"target_field": "event_id", "operation": "copy", "source_paths": ["$.activities.event_id"]},
                    {"target_field": "user_id", "operation": "copy", "source_paths": ["$.activities.user_id"]},
                    {"target_field": "activity_type", "operation": "copy", "source_paths": ["$.activities.activity_type"]},
                    {"target_field": "steps", "operation": "copy", "source_paths": ["$.activities.steps"]},
                    {"target_field": "profile_country", "operation": "copy", "source_paths": ["$.profiles.country"]},
                ],
                "joins": [
                    {
                        "left_path": "$.activities",
                        "right_path": "$.profiles",
                        "left_key": "user_id",
                        "right_key": "id",
                        "join_type": "left",
                    }
                ],
                "filters": [],
                "aggregations": [],
                "assumptions": ["Missing profile rows should preserve the activity under left join."],
            },
            "invariants": [
                {"type": "required_fields_present", "fields": ["event_id", "user_id", "profile_country"], "scope": "all_records"}
            ],
            "tags": ["synthetic", "primitive", "join", "left", "activity"],
            "notes": ["Synthetic join primitive.", "Bundle-shaped join task."],
            "downstream_checks": [
                {"type": "record_count_matches_expected"},
                {"type": "required_fields_present", "fields": ["event_id", "user_id", "activity_type", "steps"], "scope": "all_records"},
                {
                    "type": "field_types",
                    "fields": {
                        "event_id": "string",
                        "user_id": "string",
                        "activity_type": "string",
                        "steps": "integer",
                        "profile_country": "string-or-null",
                    },
                    "scope": "all_records",
                },
            ],
        }
    )

    # SYN_304 — left join rewards/campaigns
    specs.append(
        {
            "task_id": "SYN_304",
            "title": "Join primitive: rewards with campaigns (left join)",
            "split": "synthetic",
            "difficulty": "medium",
            "source_family": "synthetic_join_reward_campaign",
            "target_entity": "reward_with_campaign",
            "task_text": (
                "Create one canonical reward_with_campaign object per reward by joining "
                "rewards with campaigns on campaign_code = code using a left join."
            ),
            "primitive_family": "join",
            "primitive_subtype": "left",
            "lexical_perturbation": "none",
            "ambiguity_class": "single_gold",
            "composition_depth": 1,
            "difficulty_axes": {
                "num_source_objects": 2,
                "num_target_fields": 5,
                "num_joins": 1,
                "num_aggregations": 0,
                "nesting_depth": 1,
                "has_enum_normalization": False,
                "has_time_normalization": False,
            },
            "source_schema": {
                "type": "object",
                "title": "RewardCampaignBundle",
                "properties": {
                    "rewards": _array_of_objects(
                        [
                            ("reward_id", {"type": "string"}),
                            ("campaign_code", {"type": "string"}),
                            ("points", {"type": "integer"}),
                        ],
                        title="RewardsRow",
                    ),
                    "campaigns": _array_of_objects(
                        [
                            ("code", {"type": "string"}),
                            ("title", {"type": "string"}),
                            ("status", {"type": "string"}),
                        ],
                        title="CampaignsRow",
                    ),
                },
                "required": ["rewards", "campaigns"],
                "additionalProperties": False,
            },
            "target_schema": {
                "type": "object",
                "title": "RewardWithCampaign",
                "properties": {
                    "reward_id": {"type": "string"},
                    "campaign_code": {"type": "string"},
                    "campaign_title": {"type": ["string", "null"]},
                    "campaign_status": {"type": ["string", "null"]},
                    "points": {"type": "integer"},
                },
                "required": ["reward_id", "campaign_code", "campaign_title", "campaign_status", "points"],
                "additionalProperties": False,
            },
            "input_fixture": {
                "records": [
                    {
                        "rewards": [
                            {"reward_id": "rw-1", "campaign_code": "CAMP-A", "points": 10},
                            {"reward_id": "rw-2", "campaign_code": "CAMP-Z", "points": 5},
                        ],
                        "campaigns": [
                            {"code": "CAMP-A", "title": "Spring Steps", "status": "active"},
                        ],
                    }
                ]
            },
            "expected_fixture": {
                "records": [
                    {"reward_id": "rw-1", "campaign_code": "CAMP-A", "campaign_title": "Spring Steps", "campaign_status": "active", "points": 10},
                    {"reward_id": "rw-2", "campaign_code": "CAMP-Z", "campaign_title": None, "campaign_status": None, "points": 5},
                ]
            },
            "matches": [
                {"target_field": "reward_id", "source_path": "$.rewards.reward_id", "relation": "copy"},
                {"target_field": "campaign_code", "source_path": "$.rewards.campaign_code", "relation": "copy"},
                {"target_field": "campaign_title", "source_path": "$.campaigns.title", "relation": "copy"},
                {"target_field": "campaign_status", "source_path": "$.campaigns.status", "relation": "copy"},
                {"target_field": "points", "source_path": "$.rewards.points", "relation": "copy"},
            ],
            "plan": {
                "plan_id": "SYN_304_gold",
                "task_id": "SYN_304",
                "target_entity": "reward_with_campaign",
                "field_mappings": [
                    {"target_field": "reward_id", "operation": "copy", "source_paths": ["$.rewards.reward_id"]},
                    {"target_field": "campaign_code", "operation": "copy", "source_paths": ["$.rewards.campaign_code"]},
                    {"target_field": "campaign_title", "operation": "copy", "source_paths": ["$.campaigns.title"]},
                    {"target_field": "campaign_status", "operation": "copy", "source_paths": ["$.campaigns.status"]},
                    {"target_field": "points", "operation": "copy", "source_paths": ["$.rewards.points"]},
                ],
                "joins": [
                    {
                        "left_path": "$.rewards",
                        "right_path": "$.campaigns",
                        "left_key": "campaign_code",
                        "right_key": "code",
                        "join_type": "left",
                    }
                ],
                "filters": [],
                "aggregations": [],
                "assumptions": ["Missing campaign rows should preserve the reward under left join."],
            },
            "invariants": [
                {"type": "required_fields_present", "fields": ["reward_id", "campaign_code", "points"], "scope": "all_records"}
            ],
            "tags": ["synthetic", "primitive", "join", "left", "campaign"],
            "notes": ["Synthetic join primitive.", "Bundle-shaped join task."],
            "downstream_checks": [
                {"type": "record_count_matches_expected"},
                {"type": "required_fields_present", "fields": ["reward_id", "campaign_code", "points"], "scope": "all_records"},
                {
                    "type": "field_types",
                    "fields": {
                        "reward_id": "string",
                        "campaign_code": "string",
                        "campaign_title": "string-or-null",
                        "campaign_status": "string-or-null",
                        "points": "integer",
                    },
                    "scope": "all_records",
                },
            ],
        }
    )

    return specs