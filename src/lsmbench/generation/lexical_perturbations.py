from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LexicalVariant:
    source_field: str
    perturbation: str
    note: str | None = None
    distractor_fields: tuple[str, ...] = ()


def birth_date_variants() -> list[LexicalVariant]:
    return [
        LexicalVariant(
            source_field="birth_date",
            perturbation="exact",
            note="Exact lexical match.",
        ),
        LexicalVariant(
            source_field="date_of_birth",
            perturbation="synonym",
            note="Common synonym.",
        ),
        LexicalVariant(
            source_field="dob",
            perturbation="abbreviation",
            note="Abbreviation.",
        ),
        LexicalVariant(
            source_field="date",
            perturbation="hypernym",
            note="Broader date term.",
        ),
        LexicalVariant(
            source_field="fecha_nacimiento",
            perturbation="multilingual",
            note="Spanish source term.",
        ),
        LexicalVariant(
            source_field="birth_date",
            perturbation="distractor",
            note="Correct field appears alongside a competing date-like field.",
            distractor_fields=("created_date",),
        ),
    ]


def steps_variants() -> list[LexicalVariant]:
    return [
        LexicalVariant(
            source_field="steps",
            perturbation="exact",
            note="Exact lexical match.",
        ),
        LexicalVariant(
            source_field="step_count",
            perturbation="synonym",
            note="Common synonym.",
        ),
        LexicalVariant(
            source_field="step_cnt",
            perturbation="abbreviation",
            note="Abbreviated source label.",
        ),
        LexicalVariant(
            source_field="activity_count",
            perturbation="hypernym",
            note="Broader activity-count term.",
        ),
        LexicalVariant(
            source_field="pasos",
            perturbation="multilingual",
            note="Spanish source term.",
        ),
        LexicalVariant(
            source_field="steps",
            perturbation="distractor",
            note="Correct field appears alongside a competing numeric activity field.",
            distractor_fields=("distance_meters",),
        ),
    ]