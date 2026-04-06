from __future__ import annotations

import copy
import re
from typing import Any


_SIMPLE_FIELD_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _repair_one_path(path: Any) -> tuple[Any, bool]:
    if not isinstance(path, str):
        return path, False

    original = path

    if path.startswith("$."):
        return path, False

    cleaned = path.strip()

    if cleaned.startswith("$"):
        cleaned = cleaned[1:].strip()

        if cleaned.startswith("."):
            cleaned = cleaned[1:].strip()

        if _SIMPLE_FIELD_RE.match(cleaned):
            repaired = f"$.{cleaned}"
            return repaired, repaired != original

    return path, False


def _repair_join_key(key: Any) -> tuple[Any, bool]:
    """
    Convert obvious JSONPath-like join key noise to a bare field name.
    Examples:
      '$.orders.customer_id' -> 'customer_id'
      '$.id' -> 'id'
      '$ id' -> 'id'
    """
    if not isinstance(key, str):
        return key, False

    original = key
    cleaned = key.strip()

    if cleaned.startswith("$"):
        cleaned = cleaned[1:].strip()

    if cleaned.startswith("."):
        cleaned = cleaned[1:].strip()

    if "." in cleaned:
        cleaned = cleaned.split(".")[-1]

    if _SIMPLE_FIELD_RE.match(cleaned):
        return cleaned, cleaned != original

    return key, False


def _repair_join_type(join_type: Any) -> tuple[Any, bool]:
    if not isinstance(join_type, str):
        return join_type, False

    lowered = join_type.strip().lower()
    if lowered in {"left", "inner"}:
        return lowered, lowered != join_type

    return join_type, False


def _repair_assumptions(assumptions: Any) -> tuple[Any, list[str]]:
    notes: list[str] = []

    if not isinstance(assumptions, list):
        return assumptions, notes

    repaired: list[Any] = []
    changed = False

    for i, item in enumerate(assumptions):
        if isinstance(item, str):
            repaired.append(item)
            continue

        if isinstance(item, dict):
            if isinstance(item.get("description"), str):
                repaired.append(item["description"])
                changed = True
                notes.append(
                    f"assumptions[{i}] repaired from object to description string"
                )
                continue

        repaired.append(item)

    return repaired if changed else assumptions, notes


def repair_plan_paths(plan: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    """
    Deterministically repair only obvious plan formatting mistakes.
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

        for key in ("left_key", "right_key"):
            p = join.get(key)
            new_p, changed = _repair_join_key(p)
            if changed:
                join[key] = new_p
                notes.append(f"joins[{i}].{key} repaired: {p!r} -> {new_p!r}")

        jt = join.get("join_type")
        new_jt, changed = _repair_join_type(jt)
        if changed:
            join["join_type"] = new_jt
            notes.append(f"joins[{i}].join_type repaired: {jt!r} -> {new_jt!r}")

    assumptions, assumption_notes = _repair_assumptions(repaired.get("assumptions", []))
    repaired["assumptions"] = assumptions
    notes.extend(assumption_notes)

    return repaired, notes