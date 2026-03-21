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


def public_openapi_tasks() -> list[dict[str, Any]]:
    """
    Generic public/OpenAPI-style tasks.

    We start with one grouped finance/payment task because the grouped execution
    engine is now available and we want the public angle to use it too.
    """
    return [
        {
            "task_id": "PUB_105",
            "title": "Map payment events to customer_payment_summary",
            "difficulty": "medium",
            "source_family": "openapi_payment_event",
            "target_entity": "customer_payment_summary",
            "task_text": (
                "Create one customer_payment_summary JSON object per customer "
                "from a bundle of payment event records."
            ),
            "source_schema": _obj(
                ["event_id", "customer_id", "currency", "status", "created_at", "amount_minor"],
                {
                    "event_id": {"type": "string"},
                    "customer_id": {"type": "string"},
                    "currency": {"type": "string"},
                    "status": {"type": "string"},
                    "created_at": {"type": "string"},
                    "amount_minor": {"type": "integer"},
                },
                title="PaymentEventRecord",
            ),
            "target_schema": _obj(
                [
                    "customer_id",
                    "currency",
                    "payment_count",
                    "total_amount_minor",
                    "first_payment_time",
                    "last_payment_time",
                ],
                {
                    "customer_id": {"type": "string"},
                    "currency": {"type": "string"},
                    "payment_count": {"type": "integer"},
                    "total_amount_minor": {"type": "number"},
                    "first_payment_time": {"type": "string"},
                    "last_payment_time": {"type": "string"},
                },
                title="CustomerPaymentSummary",
            ),
            "input_fixture": {
                "records": [
                    {
                        "event_id": "pay-001",
                        "customer_id": "cust-1",
                        "currency": "EUR",
                        "status": "succeeded",
                        "created_at": "2026-02-13T10:00:00Z",
                        "amount_minor": 1200,
                    },
                    {
                        "event_id": "pay-002",
                        "customer_id": "cust-1",
                        "currency": "EUR",
                        "status": "succeeded",
                        "created_at": "2026-02-13T11:15:00Z",
                        "amount_minor": 800,
                    },
                    {
                        "event_id": "pay-003",
                        "customer_id": "cust-2",
                        "currency": "EUR",
                        "status": "failed",
                        "created_at": "2026-02-13T14:05:00Z",
                        "amount_minor": 500,
                    },
                ]
            },
            "expected_fixture": {
                "records": [
                    {
                        "customer_id": "cust-1",
                        "currency": "EUR",
                        "payment_count": 2,
                        "total_amount_minor": 2000.0,
                        "first_payment_time": "2026-02-13T10:00:00Z",
                        "last_payment_time": "2026-02-13T11:15:00Z",
                    },
                    {
                        "customer_id": "cust-2",
                        "currency": "EUR",
                        "payment_count": 1,
                        "total_amount_minor": 500.0,
                        "first_payment_time": "2026-02-13T14:05:00Z",
                        "last_payment_time": "2026-02-13T14:05:00Z",
                    },
                ]
            },
            "matches": [
                {
                    "target_field": "customer_id",
                    "source_path": "$.customer_id",
                    "relation": "group_key",
                },
                {
                    "target_field": "currency",
                    "source_path": "$.currency",
                    "relation": "exact:first_record_in_group",
                },
                {
                    "target_field": "payment_count",
                    "source_path": "$.event_id",
                    "relation": "aggregate:count",
                },
                {
                    "target_field": "total_amount_minor",
                    "source_path": "$.amount_minor",
                    "relation": "aggregate:sum",
                },
                {
                    "target_field": "first_payment_time",
                    "source_path": "$.created_at",
                    "relation": "aggregate:first",
                },
                {
                    "target_field": "last_payment_time",
                    "source_path": "$.created_at",
                    "relation": "aggregate:latest",
                },
            ],
            "plan": {
                "plan_id": "PUB_105_gold",
                "task_id": "PUB_105",
                "target_entity": "customer_payment_summary",
                "group_by_paths": ["$.customer_id"],
                "field_mappings": [
                    {
                        "target_field": "customer_id",
                        "operation": "copy",
                        "source_paths": ["$.customer_id"],
                    },
                    {
                        "target_field": "currency",
                        "operation": "copy",
                        "source_paths": ["$.currency"],
                    },
                ],
                "joins": [],
                "filters": [],
                "aggregations": [
                    {
                        "target_field": "payment_count",
                        "function": "count",
                        "source_path": "$.event_id",
                    },
                    {
                        "target_field": "total_amount_minor",
                        "function": "sum",
                        "source_path": "$.amount_minor",
                    },
                    {
                        "target_field": "first_payment_time",
                        "function": "first",
                        "source_path": "$.created_at",
                    },
                    {
                        "target_field": "last_payment_time",
                        "function": "latest",
                        "source_path": "$.created_at",
                    },
                ],
                "assumptions": [
                    "Records are grouped by customer_id.",
                    "Within a group, input order reflects event order.",
                    "currency is read from the first record in the group.",
                ],
                "confidence": 1.0,
            },
            "invariants": [
                {"type": "non_empty", "field": "customer_id"},
                {"type": "field_type", "field": "currency", "expect": "string"},
                {"type": "range", "field": "payment_count", "min": 1},
                {"type": "range", "field": "total_amount_minor", "min": 0},
            ],
            "downstream_checks": [
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
                {
                    "type": "field_types",
                    "fields": {
                        "customer_id": "string",
                        "currency": "string",
                        "payment_count": "integer",
                        "total_amount_minor": "number",
                        "first_payment_time": "datetime-string",
                        "last_payment_time": "datetime-string",
                    },
                    "scope": "all_records",
                },
            ],
            "tags": [
                "public",
                "openapi",
                "finance",
                "payment",
                "grouped-execution",
                "aggregation",
            ],
        },
        {
            "task_id": "PUB_106",
            "title": "Map joined order and customer bundles to order_with_customer",
            "difficulty": "medium",
            "source_family": "openapi_order_customer_bundle",
            "target_entity": "order_with_customer",
            "task_text": (
                "Create one canonical order_with_customer JSON object per order "
                "by joining orders with customers on customer_id."
            ),
            "source_schema": _obj(
                ["orders", "customers"],
                {
                    "orders": {
                        "type": "array",
                        "items": _obj(
                            ["id", "customer_id", "status", "total_amount"],
                            {
                                "id": {"type": "string"},
                                "customer_id": {"type": "string"},
                                "status": {"type": "string"},
                                "total_amount": {"type": "number"},
                            },
                        ),
                    },
                    "customers": {
                        "type": "array",
                        "items": _obj(
                            ["id", "email", "country", "tier"],
                            {
                                "id": {"type": "string"},
                                "email": {"type": "string"},
                                "country": {"type": "string"},
                                "tier": {"type": "string"},
                            },
                        ),
                    },
                },
                title="OrderCustomerBundle",
            ),
            "target_schema": _obj(
                [
                    "order_id",
                    "customer_id",
                    "customer_email",
                    "customer_country",
                    "customer_tier",
                    "order_status",
                    "total_amount",
                ],
                {
                    "order_id": {"type": "string"},
                    "customer_id": {"type": "string"},
                    "customer_email": {"type": ["string", "null"]},
                    "customer_country": {"type": ["string", "null"]},
                    "customer_tier": {"type": ["string", "null"]},
                    "order_status": {"type": "string"},
                    "total_amount": {"type": "number"},
                },
                title="OrderWithCustomer",
            ),
            "input_fixture": {
                "records": [
                    {
                        "orders": [
                            {
                                "id": "ord-001",
                                "customer_id": "cust-1",
                                "status": "paid",
                                "total_amount": 24.0,
                            },
                            {
                                "id": "ord-002",
                                "customer_id": "cust-2",
                                "status": "pending",
                                "total_amount": 11.5,
                            },
                            {
                                "id": "ord-003",
                                "customer_id": "cust-9",
                                "status": "paid",
                                "total_amount": 8.0,
                            },
                        ],
                        "customers": [
                            {
                                "id": "cust-1",
                                "email": "a@example.org",
                                "country": "NL",
                                "tier": "gold",
                            },
                            {
                                "id": "cust-2",
                                "email": "b@example.org",
                                "country": "DE",
                                "tier": "silver",
                            },
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
                {
                    "target_field": "order_id",
                    "source_path": "$.orders.id",
                    "relation": "exact",
                },
                {
                    "target_field": "customer_id",
                    "source_path": "$.orders.customer_id",
                    "relation": "exact",
                },
                {
                    "target_field": "customer_email",
                    "source_path": "$.customers.email",
                    "relation": "exact:left_join",
                },
                {
                    "target_field": "customer_country",
                    "source_path": "$.customers.country",
                    "relation": "exact:left_join",
                },
                {
                    "target_field": "customer_tier",
                    "source_path": "$.customers.tier",
                    "relation": "exact:left_join",
                },
                {
                    "target_field": "order_status",
                    "source_path": "$.orders.status",
                    "relation": "exact",
                },
                {
                    "target_field": "total_amount",
                    "source_path": "$.orders.total_amount",
                    "relation": "exact",
                },
            ],
            "plan": {
                "plan_id": "PUB_106_gold",
                "task_id": "PUB_106",
                "target_entity": "order_with_customer",
                "field_mappings": [
                    {
                        "target_field": "order_id",
                        "operation": "copy",
                        "source_paths": ["$.orders.id"],
                    },
                    {
                        "target_field": "customer_id",
                        "operation": "copy",
                        "source_paths": ["$.orders.customer_id"],
                    },
                    {
                        "target_field": "customer_email",
                        "operation": "copy",
                        "source_paths": ["$.customers.email"],
                    },
                    {
                        "target_field": "customer_country",
                        "operation": "copy",
                        "source_paths": ["$.customers.country"],
                    },
                    {
                        "target_field": "customer_tier",
                        "operation": "copy",
                        "source_paths": ["$.customers.tier"],
                    },
                    {
                        "target_field": "order_status",
                        "operation": "copy",
                        "source_paths": ["$.orders.status"],
                    },
                    {
                        "target_field": "total_amount",
                        "operation": "copy",
                        "source_paths": ["$.orders.total_amount"],
                    },
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
                "confidence": 1.0,
            },
            "invariants": [
                {"type": "non_empty", "field": "order_id"},
                {"type": "non_empty", "field": "customer_id"},
                {"type": "field_type", "field": "order_status", "expect": "string"},
                {"type": "field_type", "field": "total_amount", "expect": "number"},
            ],
            "downstream_checks": [
                {"type": "record_count_matches_expected"},
                {
                    "type": "required_fields_present",
                    "fields": [
                        "order_id",
                        "customer_id",
                        "order_status",
                        "total_amount",
                    ],
                    "scope": "all_records",
                },
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
            "tags": [
                "public",
                "openapi",
                "join",
                "commerce",
                "customer",
                "order",
            ],
        }
    ]