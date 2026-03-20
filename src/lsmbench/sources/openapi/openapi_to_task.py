from __future__ import annotations

from typing import Any


def public_openapi_tasks() -> list[dict[str, Any]]:
    """
    Placeholder for generic public/OpenAPI tasks.

    We keep this explicit so build_public_tasks.py can safely import it
    even before the first non-FHIR public tasks are added.
    """
    return []