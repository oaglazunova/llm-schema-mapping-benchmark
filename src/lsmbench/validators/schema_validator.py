from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator


REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_DIR = REPO_ROOT / "schemas"


def _load_json(path: Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_schema(schema_filename: str) -> dict[str, Any]:
    path = SCHEMA_DIR / schema_filename
    if not path.exists():
        raise FileNotFoundError(f"Schema file not found: {path}")
    return _load_json(path)


def _sorted_error_messages(validator: Draft202012Validator, instance: dict[str, Any]) -> list[str]:
    errors = sorted(validator.iter_errors(instance), key=lambda e: list(e.path))
    messages: list[str] = []
    for err in errors:
        path = "$"
        if err.path:
            path += "".join(
                f"[{p}]" if isinstance(p, int) else f".{p}"
                for p in err.path
            )
        messages.append(f"{path}: {err.message}")
    return messages


def _strip_loader_metadata(instance: dict[str, Any]) -> dict[str, Any]:
    cleaned = copy.deepcopy(instance)
    cleaned.pop("__task_path__", None)
    cleaned.pop("__task_set_path__", None)
    return cleaned


def validate_instance_against_schema(
    instance: dict[str, Any],
    schema_filename: str,
) -> dict[str, Any]:
    schema = load_schema(schema_filename)
    validator = Draft202012Validator(schema)
    cleaned = _strip_loader_metadata(instance)
    errors = _sorted_error_messages(validator, cleaned)

    return {
        "valid": len(errors) == 0,
        "schema": schema_filename,
        "errors": errors,
    }


def validate_task(task: dict[str, Any]) -> dict[str, Any]:
    return validate_instance_against_schema(task, "benchmark_task_v1.schema.json")


def validate_plan(plan: dict[str, Any]) -> dict[str, Any]:
    return validate_instance_against_schema(plan, "mapping_plan_v1.schema.json")