from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.request
from pathlib import Path
from typing import Any
from datetime import datetime, timezone

REPO_ROOT = Path(__file__).resolve().parents[1]


def load_json(path: str | Path) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: str | Path, payload: Any) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def strip_json_fences(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    return text.strip()


def to_iso_utc(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def build_prompt(task: dict[str, Any]) -> list[dict[str, str]]:
    system = (
        "You are a schema mapping assistant. "
        "Return ONLY one valid JSON object representing a mapping plan. "
        "Do not use markdown fences. "
        "Do not add explanations."
    )

    task_text = task["task_text"]
    primitive_family = task.get("primitive_family", "")
    primitive_subtype = task.get("primitive_subtype", "")

    instruction_parts = [
        "Produce a mapping plan JSON with keys: "
        "plan_id, task_id, target_entity, field_mappings, joins, filters, aggregations, assumptions.",
        "Each field_mapping must contain: target_field, operation, source_paths.",
        "target_field must be a bare field name like birth_date or steps, never a JSONPath.",
        "Every source path must be a JSONPath starting with '$.'.",
        "Do not write source paths like '$ field'; always write '$.field_name'.",
        "Use only these operations when appropriate: "
        "copy, rename, cast_string, cast_integer, cast_number, cast_boolean, "
        "parse_date, parse_datetime, truncate_date, normalize_enum, normalize_boolean, "
        "concat, split, derive_arithmetic, default_value, coalesce, latest_value.",
    ]

    needs_parameters = (
        primitive_subtype in {"normalize_enum", "normalize_boolean"}
        or "Use these operation parameters:" in task_text
    )

    if needs_parameters:
        instruction_parts.append(
            "A field_mapping may include a parameters object when the operation requires it."
        )
        instruction_parts.append(
            "For normalize_enum, include parameters.mapping when the task provides an explicit mapping."
        )
        instruction_parts.append(
            "For normalize_boolean, include parameters.truthy_values and parameters.falsy_values "
            "when the task provides them."
        )

    if primitive_family == "join":
        instruction_parts.append(
            "If the task requires combining arrays from the source bundle, include a joins list."
        )
        instruction_parts.append(
            "Each join must include left_path, right_path, left_key, right_key, and join_type."
        )
        instruction_parts.append(
            "Use join paths like '$.orders' and '$.customers' for bundle arrays."
        )
        instruction_parts.append(
            "left_key and right_key must be bare field names like 'customer_id' or 'id', never JSONPaths."
        )
        instruction_parts.append(
            "join_type must be lowercase, for example 'left' or 'inner'."
        )
        instruction_parts.append(
            "After joining, field_mappings must use simple alias paths like '$.orders.id' or '$.customers.email'."
        )
        instruction_parts.append(
            "Do not put join logic inside source_paths. Do not use filtered or predicate JSONPath such as '[?()]' or '[id == ...]'."
        )
        instruction_parts.append(
            "Use copy for joined fields unless the task explicitly requires another operation."
        )
        instruction_parts.append(
            "assumptions must be a list of plain strings, not objects."
        )
        instruction_parts.append(
            "Example: if joins contains "
            "{left_path:'$.orders', right_path:'$.customers', left_key:'customer_id', right_key:'id', join_type:'left'}, "
            "then a joined customer email field must use source_paths ['$.customers.email'], not a filtered path."
        )

    user = {
        "instruction": " ".join(instruction_parts),
        "task_id": task["task_id"],
        "title": task["title"],
        "task_text": task_text,
        "target_entity": task["target_entity"],
        "source_schema": task["source_schema"],
        "target_schema": task["target_schema"],
    }

    return [
        {"role": "system", "content": system},
        {"role": "user", "content": json.dumps(user, ensure_ascii=False, indent=2)},
    ]


def prompt_track_for_task(task: dict[str, Any]) -> str:
    primitive_subtype = task.get("primitive_subtype", "")
    task_text = task.get("task_text", "")

    if task.get("primitive_family") == "join":
        return "join_example_v1"

    if (
        primitive_subtype in {"normalize_enum", "normalize_boolean"}
        or "Use these operation parameters:" in task_text
    ):
        return "parameter_aware_v1"

    return "minimal_v1"


def call_ollama(model: str, messages: list[dict[str, str]]) -> dict[str, Any]:
    payload = {
        "model": model,
        "messages": messages,
        "stream": False,
    }

    req = urllib.request.Request(
        "http://localhost:11434/api/chat",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode("utf-8"))


def parse_candidate_plan(raw_text: str) -> tuple[dict[str, Any] | None, str | None]:
    cleaned = strip_json_fences(raw_text)
    try:
        parsed = json.loads(cleaned)
        if isinstance(parsed, dict):
            return parsed, None
        return None, "parsed_output_not_object"
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one benchmark task against local Ollama.")
    parser.add_argument("--task-path", required=True)
    parser.add_argument("--task-set-id", required=True)
    parser.add_argument("--run-id", default="run_001")
    parser.add_argument("--model", default="qwen2.5:7b-instruct-q4_0")
    args = parser.parse_args()

    task = load_json(args.task_path)
    messages = build_prompt(task)

    started_at = time.time()
    response = call_ollama(args.model, messages)
    finished_at = time.time()

    raw_text = response.get("message", {}).get("content", "")
    candidate_plan, parse_error = parse_candidate_plan(raw_text)

    model_slug = args.model.replace(":", "__")
    out_path = (
        REPO_ROOT
        / "runs"
        / model_slug
        / args.task_set_id
        / args.run_id
        / "task_results"
        / f"{task['task_id']}.json"
    )

    result = {
        "schema_version": "model_run_record_v1",
        "run_id": args.run_id,
        "task_set_id": args.task_set_id,
        "task_id": task["task_id"],
        "model": {
            "model_name": args.model,
            "provider": "ollama",
        },
        "prompt": {
            "track": prompt_track_for_task(task),
            "template_id": "dev_run_one_task_ollama_v2",
            "template_version": "0.2.0",
        },
        "generation": {
            "temperature": 0.0,
        },
        "status": "ok" if candidate_plan is not None else "invalid_output",
        "selected_candidate_id": "c1",
        "candidates": [
            {
                "candidate_id": "c1",
                "raw_text": raw_text,
                "candidate_plan": candidate_plan,
                "parse_error": parse_error,
            }
        ],
        "usage": {
            "latency_ms": round((finished_at - started_at) * 1000.0, 3)
        },
        "timestamps": {
            "started_at": to_iso_utc(started_at),
            "finished_at": to_iso_utc(finished_at),
        },
        "notes": [],
    }

    write_json(out_path, result)

    print(f"Wrote result file: {out_path}")
    print("")
    print("Raw model output:")
    print(raw_text[:2000])

    return 0


if __name__ == "__main__":
    raise SystemExit(main())