from __future__ import annotations

import copy
import re
from typing import Any


_SIMPLE_FIELD_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _repair_one_path(path: Any) -> tuple[Any, bool]:
    if not isinstance(path, str):
        return path, False

    original = path

    # Already valid simple JSONPath like $.field_name
    if path.startswith("$."):
        return path, False

    # Common bad forms:
    # "$ field"        -> "$.field"
    # "$field"         -> "$.field"
    # "$ .field"       -> "$.field"
    # "$. field"       -> "$.field"
    cleaned = path.strip()

    if cleaned.startswith("$"):
        cleaned = cleaned[1:].strip()

        if cleaned.startswith("."):
            cleaned = cleaned[1:].strip()

        if _SIMPLE_FIELD_RE.match(cleaned):
            repaired = f"$.{cleaned}"
            return repaired, repaired != original

    return path, False


def repair_plan_paths(plan: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    """
    Deterministically repair only obvious JSONPath formatting mistakes.
    Returns a deep-copied repaired plan and a list of repair notes.
    """
    repaired = copy.deepcopy(plan)
    notes: list[str] = []

    for i, fm in enumerate(repaired.get("field_mappings", [])):
        source_paths = fm.get("source_paths", [])
        if not isinstance(source_paths, list):
            continue

        new_paths = []
        for j, p in enumerate(source_paths):
            new_p, changed = _repair_one_path(p)
            new_paths.append(new_p)
            if changed:
                notes.append(
                    f"field_mappings[{i}].source_paths[{j}] repaired: {p!r} -> {new_p!r}"
                )
        fm["source_paths"] = new_paths

    for i, flt in enumerate(repaired.get("filters", [])):
        p = flt.get("path")
        new_p, changed = _repair_one_path(p)
        if changed:
            flt["path"] = new_p
            notes.append(f"filters[{i}].path repaired: {p!r} -> {new_p!r}")

    for i, agg in enumerate(repaired.get("aggregations", [])):
        p = agg.get("source_path")
        new_p, changed = _repair_one_path(p)
        if changed:
            agg["source_path"] = new_p
            notes.append(f"aggregations[{i}].source_path repaired: {p!r} -> {new_p!r}")

        group_by = agg.get("group_by", [])
        if isinstance(group_by, list):
            new_group_by = []
            for j, p in enumerate(group_by):
                new_p, changed = _repair_one_path(p)
                new_group_by.append(new_p)
                if changed:
                    notes.append(
                        f"aggregations[{i}].group_by[{j}] repaired: {p!r} -> {new_p!r}"
                    )
            agg["group_by"] = new_group_by

    for i, join in enumerate(repaired.get("joins", [])):
        for key in ("left_path", "right_path"):
            p = join.get(key)
            new_p, changed = _repair_one_path(p)
            if changed:
                join[key] = new_p
                notes.append(f"joins[{i}].{key} repaired: {p!r} -> {new_p!r}")

    return repaired, notes