from __future__ import annotations

from lsmbench.validators.downstream_validator import _check_field_types


def test_check_field_types_all_records_passes():
    produced = [
        {
            "customer_id": "c1",
            "currency": "EUR",
            "payment_count": 2,
            "total_amount_minor": 1234,
            "first_payment_time": "2025-01-01T10:00:00",
            "last_payment_time": "2025-01-02T12:00:00",
        }
    ]

    fields = {
        "customer_id": "string",
        "currency": "string",
        "payment_count": "integer",
        "total_amount_minor": "number",
        "first_payment_time": "datetime-string",
        "last_payment_time": "datetime-string",
    }

    errors = _check_field_types(produced, fields, scope="all_records")
    assert errors == []


def test_check_field_types_all_records_fails_on_wrong_type():
    produced = [
        {
            "customer_id": "c1",
            "payment_count": "2",  # wrong type
        }
    ]

    fields = {
        "customer_id": "string",
        "payment_count": "integer",
    }

    errors = _check_field_types(produced, fields, scope="all_records")
    assert errors
    assert "payment_count" in errors[0]