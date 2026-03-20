from __future__ import annotations

import re
from typing import Any

_JSONPATH_TOKEN_RE = re.compile(
    r"""
    (?:
        \.([A-Za-z_][A-Za-z0-9_]*)
    )
    |
    (?:
        \[(\*|\d+)\]
    )
    """,
    re.VERBOSE,
)


def tokenize_json_path(path: str) -> list[tuple[str, str]]:
    if not path.startswith("$"):
        raise ValueError(f"Path must start with '$': {path}")

    tokens: list[tuple[str, str]] = []
    for match in _JSONPATH_TOKEN_RE.finditer(path[1:]):
        prop, idx = match.groups()
        if prop is not None:
            tokens.append(("prop", prop))
        elif idx is not None:
            tokens.append(("index", idx))
    return tokens


def resolve_all(data: Any, path: str) -> list[Any]:
    """
    Resolve a minimal JSONPath subset and return all matching values.

    Supported:
    - $.field
    - $.field.subfield
    - $.array[0]
    - $.array[*]
    - $.array[*].field
    """
    tokens = tokenize_json_path(path)
    current: list[Any] = [data]

    for kind, value in tokens:
        next_values: list[Any] = []

        if kind == "prop":
            for item in current:
                if isinstance(item, dict) and value in item:
                    next_values.append(item[value])

        elif kind == "index":
            for item in current:
                if not isinstance(item, list):
                    continue

                if value == "*":
                    next_values.extend(item)
                else:
                    idx = int(value)
                    if 0 <= idx < len(item):
                        next_values.append(item[idx])

        current = next_values

    return current


def resolve_one(data: Any, path: str) -> Any:
    values = resolve_all(data, path)
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    return values


def get_nested(obj: Any, dotted_path: str) -> Any:
    """
    Resolve dotted object paths inside already-extracted Python objects.

    Example:
    - code.text
    - answer
    - valueInteger
    """
    current = obj
    for part in dotted_path.split("."):
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return None
    return current