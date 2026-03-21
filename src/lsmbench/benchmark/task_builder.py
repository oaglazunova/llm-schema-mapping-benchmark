from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _infer_scalar_schema(value: Any) -> dict[str, Any]:
    if value is None:
        return {"type": "null"}
    if isinstance(value, bool):
        return {"type": "boolean"}
    if isinstance(value, int) and not isinstance(value, bool):
        return {"type": "integer"}
    if isinstance(value, float):
        return {"type": "number"}
    if isinstance(value, str):
        return {"type": "string"}
    return {"type": "string"}


def _merge_types(types: list[str]) -> str | list[str]:
    unique = []
    for t in types:
        if t not in unique:
            unique.append(t)
    if len(unique) == 1:
        return unique[0]
    return unique


def infer_schema_from_example(value: Any, *, title: str | None = None) -> dict[str, Any]:
    """
    Infer a lightweight JSON Schema from one example value.

    This is intentionally conservative and intended for benchmark scaffolding,
    not for precise production-grade schema inference.
    """
    if isinstance(value, dict):
        props: dict[str, Any] = {}
        required: list[str] = []

        for key, child in value.items():
            props[key] = infer_schema_from_example(child)
            required.append(key)

        out = {
            "type": "object",
            "properties": props,
            "required": required,
            "additionalProperties": False,
        }
        if title:
            out["title"] = title
        return out

    if isinstance(value, list):
        if not value:
            out = {
                "type": "array",
                "items": {},
            }
            if title:
                out["title"] = title
            return out

        # If all items are dicts, merge object fields conservatively
        if all(isinstance(item, dict) for item in value):
            all_keys = sorted({k for item in value for k in item.keys()})
            props: dict[str, Any] = {}

            for key in all_keys:
                child_values = [item.get(key) for item in value if key in item]
                child_schemas = [infer_schema_from_example(v) for v in child_values]

                # Simple merge strategy:
                # if all schemas are objects with same type, keep one representative
                child_types = [schema.get("type", "string") for schema in child_schemas]
                merged_type = _merge_types([t if isinstance(t, str) else "string" for t in child_types])

                if all(schema.get("type") == "object" for schema in child_schemas):
                    # fall back to first object schema for simplicity
                    props[key] = child_schemas[0]
                elif all(schema.get("type") == "array" for schema in child_schemas):
                    props[key] = child_schemas[0]
                else:
                    props[key] = {"type": merged_type}

            item_schema = {
                "type": "object",
                "properties": props,
                "required": all_keys,
                "additionalProperties": False,
            }
        else:
            child_schemas = [infer_schema_from_example(item) for item in value]
            child_types = [schema.get("type", "string") for schema in child_schemas]
            merged_type = _merge_types([t if isinstance(t, str) else "string" for t in child_types])
            item_schema = {"type": merged_type}

        out = {
            "type": "array",
            "items": item_schema,
        }
        if title:
            out["title"] = title
        return out

    out = _infer_scalar_schema(value)
    if title:
        out["title"] = title
    return out


def build_task_skeleton(
    *,
    task_id: str,
    title: str,
    split: str,
    difficulty: str,
    source_family: str,
    target_entity: str,
    task_text: str,
    source_schema: dict[str, Any],
    target_schema: dict[str, Any],
    input_fixture_ref: str,
    expected_fixture_ref: str,
    tags: list[str] | None = None,
    notes: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "task_id": task_id,
        "title": title,
        "split": split,
        "difficulty": difficulty,
        "source_family": source_family,
        "target_entity": target_entity,
        "task_text": task_text,
        "source_schema": source_schema,
        "target_schema": target_schema,
        "fixture_refs": {
            "input": input_fixture_ref,
            "expected": expected_fixture_ref,
        },
        "gold_refs": {
            "matches": f"benchmark/gold/matches/{task_id}_matches.json",
            "plan": f"benchmark/gold/plans/{task_id}_plan.json",
            "invariants": f"benchmark/gold/invariants/{task_id}_invariants.json",
        },
        "tags": tags or [],
        "notes": notes or [],
    }


def build_empty_matches() -> list[dict[str, Any]]:
    return []


def build_empty_plan(*, task_id: str, target_entity: str) -> dict[str, Any]:
    return {
        "plan_id": f"{task_id}_gold",
        "task_id": task_id,
        "target_entity": target_entity,
        "field_mappings": [],
        "joins": [],
        "filters": [],
        "aggregations": [],
        "assumptions": [],
        "confidence": 1.0,
    }


def build_empty_invariants() -> list[dict[str, Any]]:
    return []


def load_fixture(path: str | Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def first_record_from_fixture(fixture: dict[str, Any]) -> dict[str, Any]:
    records = fixture.get("records", [])
    if not isinstance(records, list) or not records:
        raise ValueError("Fixture must contain a non-empty 'records' list.")
    first = records[0]
    if not isinstance(first, dict):
        raise ValueError("Fixture 'records[0]' must be an object.")
    return first


def write_json(path: str | Path, obj: Any) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)