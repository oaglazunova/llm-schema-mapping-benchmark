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

    user = {
        "instruction": (
            "Produce a mapping plan JSON with keys: "
            "plan_id, task_id, target_entity, field_mappings, joins, filters, aggregations, assumptions. "
            "Each field_mapping must contain: target_field, operation, source_paths. "
            "Every source path must be a JSONPath starting with '$.'. "
            "Use only these operations when appropriate: "
            "copy, rename, cast_string, cast_integer, cast_number, cast_boolean, "
            "parse_date, parse_datetime, truncate_date, normalize_enum, normalize_boolean, "
            "concat, split, derive_arithmetic, default_value, coalesce, latest_value. "
            "Do not use markdown fences. Return only one JSON object."
        ),
        "task_id": task["task_id"],
        "title": task["title"],
        "task_text": task["task_text"],
        "target_entity": task["target_entity"],
        "source_schema": task["source_schema"],
        "target_schema": task["target_schema"],
    }

    return [
        {"role": "system", "content": system},
        {"role": "user", "content": json.dumps(user, ensure_ascii=False, indent=2)},
    ]


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
            "track": "manual_dev",
            "template_id": "dev_run_one_task_ollama_v1",
            "template_version": "0.1.0",
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