from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class TypeNormalizationVariant:
    source_field: str
    target_field: str
    operation: str
    source_value: Any
    expected_value: Any
    perturbation: str
    note: str | None = None
    distractor_fields: tuple[str, ...] = ()
    target_entity: str = ""
    source_type: dict[str, Any] | None = None
    target_type: dict[str, Any] | None = None
    parameters: dict[str, Any] | None = None


def type_normalization_variants() -> list[TypeNormalizationVariant]:
    return [
        TypeNormalizationVariant(
            source_field="dob_text",
            target_field="birth_date",
            operation="parse_date",
            source_value="1992-03-14",
            expected_value="1992-03-14",
            perturbation="date_text_iso",
            note="ISO date string to canonical birth_date.",
            target_entity="canonical_user_profile",
            source_type={"type": "string"},
            target_type={"type": "string"},
        ),
        TypeNormalizationVariant(
            source_field="fecha_nacimiento",
            target_field="birth_date",
            operation="parse_date",
            source_value="1992-03-14",
            expected_value="1992-03-14",
            perturbation="date_text_multilingual",
            note="Spanish field name, ISO date value.",
            target_entity="canonical_user_profile",
            source_type={"type": "string"},
            target_type={"type": "string"},
        ),
        TypeNormalizationVariant(
            source_field="step_count_text",
            target_field="steps",
            operation="cast_integer",
            source_value="7421",
            expected_value=7421,
            perturbation="numeric_string",
            note="Numeric string to integer.",
            target_entity="canonical_activity_summary",
            source_type={"type": "string"},
            target_type={"type": "integer"},
        ),
        TypeNormalizationVariant(
            source_field="pasos_texto",
            target_field="steps",
            operation="cast_integer",
            source_value="7421",
            expected_value=7421,
            perturbation="numeric_string_multilingual",
            note="Spanish field name, numeric string value.",
            target_entity="canonical_activity_summary",
            source_type={"type": "string"},
            target_type={"type": "integer"},
        ),
        TypeNormalizationVariant(
            source_field="marketing_opt_in_text",
            target_field="marketing_opt_in",
            operation="normalize_boolean",
            source_value="yes",
            expected_value=True,
            perturbation="boolean_yes_no",
            note="yes/no text normalized to boolean.",
            target_entity="canonical_user_profile",
            source_type={"type": "string"},
            target_type={"type": "boolean"},
            parameters={
                "truthy_values": ["yes", "true", "1", "y"],
                "falsy_values": ["no", "false", "0", "n"]
            },
        ),
        TypeNormalizationVariant(
            source_field="gender_code",
            target_field="gender",
            operation="normalize_enum",
            source_value="F",
            expected_value="female",
            perturbation="enum_code",
            note="One-letter code normalized to canonical enum.",
            target_entity="canonical_user_profile",
            source_type={"type": "string"},
            target_type={"type": "string"},
            parameters={
                "mapping": {
                    "F": "female",
                    "M": "male",
                    "U": "unknown"
                }
            },
        ),
        TypeNormalizationVariant(
            source_field="activity_time",
            target_field="activity_date",
            operation="truncate_date",
            source_value="2025-03-30T10:15:00Z",
            expected_value="2025-03-30",
            perturbation="datetime_to_date",
            note="Datetime normalized to date.",
            target_entity="canonical_activity_summary",
            source_type={"type": "string"},
            target_type={"type": "string"},
        ),
        TypeNormalizationVariant(
            source_field="activity_time",
            target_field="activity_date",
            operation="truncate_date",
            source_value="2025-03-30T10:15:00Z",
            expected_value="2025-03-30",
            perturbation="datetime_to_date_with_distractor",
            note="Datetime normalized to date with distractor field present.",
            distractor_fields=("created_at",),
            target_entity="canonical_activity_summary",
            source_type={"type": "string"},
            target_type={"type": "string"},
        ),
    ]