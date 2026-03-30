from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LexicalVariant:
    target_label: str
    source_label: str
    perturbation: str
    note: str | None = None


def copy_rename_variants() -> list[LexicalVariant]:
    return [
        LexicalVariant(
            target_label="birth_date",
            source_label="birth_date",
            perturbation="exact",
            note="Exact lexical match."
        ),
        LexicalVariant(
            target_label="birth_date",
            source_label="date_of_birth",
            perturbation="synonym",
            note="Common synonym."
        ),
        LexicalVariant(
            target_label="birth_date",
            source_label="dob",
            perturbation="abbreviation",
            note="Abbreviation."
        ),
        LexicalVariant(
            target_label="steps",
            source_label="activity_count",
            perturbation="hypernym",
            note="Broader source term."
        ),
        LexicalVariant(
            target_label="birth_date",
            source_label="fecha_nacimiento",
            perturbation="multilingual",
            note="Spanish source term."
        ),
        LexicalVariant(
            target_label="user_id",
            source_label="participant_id",
            perturbation="distractor",
            note="Close competing identifier."
        ),
    ]