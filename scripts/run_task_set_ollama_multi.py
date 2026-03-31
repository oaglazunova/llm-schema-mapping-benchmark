from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from lsmbench.benchmark.task_loader import load_tasks_from_task_set


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



def build_generation_prompt(task: dict[str, Any]) -> list[dict[str, str]]:
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


def build_self_select_prompt(task: dict[str, Any], parsed_candidates: list[dict[str, Any]]) -> list[dict[str, str]]:
    system = (
        "You are a schema mapping evaluator. "
        "Choose the best candidate plan for the task. "
        "Return ONLY a JSON object of the form {\"selected_candidate_id\": \"...\"}. "
        "Do not use markdown fences. "
        "Do not add explanations."
    )

    user = {
        "instruction": (
            "Select the best candidate mapping plan for this task. "
            "Prefer plans that match the task_id, target_entity, valid JSONPath source paths, "
            "and the simplest correct mapping."
        ),
        "task_id": task["task_id"],
        "title": task["title"],
        "task_text": task["task_text"],
        "target_entity": task["target_entity"],
        "source_schema": task["source_schema"],
        "target_schema": task["target_schema"],
        "candidates": parsed_candidates,
    }

    return [
        {"role": "system", "content": system},
        {"role": "user", "content": json.dumps(user, ensure_ascii=False, indent=2)},
    ]


def call_ollama(model: str, messages: list[dict[str, str]], *, temperature: float, seed: int | None) -> dict[str, Any]:
    payload = {
        "model": model,
        "messages": messages,
        "stream": False,
        "options": {
            "temperature": temperature,
        },
    }
    if seed is not None:
        payload["options"]["seed"] = seed

    req = urllib.request.Request(
        "http://localhost:11434/api/chat",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode("utf-8"))


def parse_json_object(raw_text: str) -> tuple[dict[str, Any] | None, str | None]:
    cleaned = strip_json_fences(raw_text)
    try:
        parsed = json.loads(cleaned)
        if isinstance(parsed, dict):
            return parsed, None
        return None, "parsed_output_not_object"
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"


def run_dir_for(model: str, task_set_id: str, run_id: str) -> Path:
    model_slug = model.replace(":", "__")
    return REPO_ROOT / "runs" / model_slug / task_set_id / run_id


def write_manifest(
    run_dir: Path,
    model: str,
    task_set_id: str,
    run_id: str,
    *,
    num_candidates: int,
    selection_strategy: str,
) -> None:
    manifest = {
        "schema_version": "run_manifest_v1",
        "run_id": run_id,
        "task_set_id": task_set_id,
        "model": {
            "model_name": model,
            "provider": "ollama"
        },
        "prompt_defaults": {
            "track": "multi_candidate_v1",
            "template_id": "run_task_set_ollama_multi_v1"
        },
        "generation_defaults": {
            "temperature": 0.7,
            "num_candidates": num_candidates,
            "selection_strategy": selection_strategy,
        }
    }
    write_json(run_dir / "run_manifest.json", manifest)


def select_candidate_first_valid_json(candidates: list[dict[str, Any]]) -> str | None:
    for cand in candidates:
        if cand.get("candidate_plan") is not None:
            return cand["candidate_id"]
    return None


def select_candidate_self_select(
    model: str,
    task: dict[str, Any],
    candidates: list[dict[str, Any]],
) -> str | None:
    parsed_candidates = [
        {
            "candidate_id": c["candidate_id"],
            "candidate_plan": c["candidate_plan"],
            "parse_error": c["parse_error"],
        }
        for c in candidates
    ]

    messages = build_self_select_prompt(task, parsed_candidates)
    response = call_ollama(model, messages, temperature=0.0, seed=None)
    raw_text = response.get("message", {}).get("content", "")
    parsed, _ = parse_json_object(raw_text)
    if not parsed:
        return None

    selected_candidate_id = parsed.get("selected_candidate_id")
    if not isinstance(selected_candidate_id, str):
        return None

    candidate_ids = {c["candidate_id"] for c in candidates}
    return selected_candidate_id if selected_candidate_id in candidate_ids else None


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a task set against local Ollama with multiple candidates.")
    parser.add_argument("--task-set", required=True)
    parser.add_argument("--task-set-id", default=None)
    parser.add_argument("--run-id", default="run_001")
    parser.add_argument("--model", default="qwen2.5:7b-instruct-q4_0")
    parser.add_argument("--max-tasks", type=int, default=None)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--num-candidates", type=int, default=3)
    parser.add_argument(
        "--selection-strategy",
        choices=["first_valid_json", "self_select"],
        default="first_valid_json",
    )
    args = parser.parse_args()

    task_set_id = args.task_set_id or Path(args.task_set).stem
    run_dir = run_dir_for(args.model, task_set_id, args.run_id)
    task_results_dir = run_dir / "task_results"
    task_results_dir.mkdir(parents=True, exist_ok=True)

    write_manifest(
        run_dir,
        args.model,
        task_set_id,
        args.run_id,
        num_candidates=args.num_candidates,
        selection_strategy=args.selection_strategy,
    )

    tasks = load_tasks_from_task_set(args.task_set)
    if args.max_tasks is not None:
        tasks = tasks[: args.max_tasks]

    print(f"Running {len(tasks)} task(s) with model={args.model}")
    print(f"Run dir: {run_dir}")
    print(f"Candidates per task: {args.num_candidates}")
    print(f"Selection strategy: {args.selection_strategy}")
    print("")

    for idx, task in enumerate(tasks, start=1):
        out_path = task_results_dir / f"{task['task_id']}.json"

        if out_path.exists() and not args.overwrite:
            print(f"[{idx}/{len(tasks)}] SKIP {task['task_id']} (already exists)")
            continue

        print(f"[{idx}/{len(tasks)}] RUN  {task['task_id']}")

        candidates: list[dict[str, Any]] = []
        started_at = time.time()

        try:
            messages = build_generation_prompt(task)

            for cand_idx in range(args.num_candidates):
                response = call_ollama(
                    args.model,
                    messages,
                    temperature=0.7,
                    seed=cand_idx + 1,
                )
                raw_text = response.get("message", {}).get("content", "")
                candidate_plan, parse_error = parse_json_object(raw_text)

                candidates.append(
                    {
                        "candidate_id": f"c{cand_idx + 1}",
                        "raw_text": raw_text,
                        "candidate_plan": candidate_plan,
                        "parse_error": parse_error,
                    }
                )

            if args.selection_strategy == "first_valid_json":
                selected_candidate_id = select_candidate_first_valid_json(candidates)
            else:
                selected_candidate_id = select_candidate_self_select(args.model, task, candidates)

            finished_at = time.time()

            result = {
                "schema_version": "model_run_record_v1",
                "run_id": args.run_id,
                "task_set_id": task_set_id,
                "task_id": task["task_id"],
                "model": {
                    "model_name": args.model,
                    "provider": "ollama",
                },
                "prompt": {
                    "track": "parameter_aware_v1"
                    if task.get("primitive_subtype") in {"normalize_enum", "normalize_boolean"}
                    or "Use these operation parameters:" in task.get("task_text", "")
                    else "minimal_v1",
                    "template_id": "run_task_set_ollama_v2",
                    "template_version": "0.2.0",
                },
                "generation": {
                    "temperature": 0.7,
                },
                "status": "ok" if selected_candidate_id is not None else "invalid_output",
                "selected_candidate_id": selected_candidate_id,
                "candidates": candidates,
                "usage": {
                    "latency_ms": round((finished_at - started_at) * 1000.0, 3)
                },
                "timestamps": {
                    "started_at": to_iso_utc(started_at),
                    "finished_at": to_iso_utc(finished_at),
                },
                "notes": [],
            }

        except Exception as e:
            finished_at = time.time()
            result = {
                "schema_version": "model_run_record_v1",
                "run_id": args.run_id,
                "task_set_id": task_set_id,
                "task_id": task["task_id"],
                "model": {
                    "model_name": args.model,
                    "provider": "ollama",
                },
                "status": "error",
                "selected_candidate_id": None,
                "candidates": [],
                "usage": {
                    "latency_ms": round((finished_at - started_at) * 1000.0, 3)
                },
                "timestamps": {
                    "started_at": to_iso_utc(started_at),
                    "finished_at": to_iso_utc(finished_at),
                },
                "error_message": f"{type(e).__name__}: {e}",
                "notes": [],
            }

        write_json(out_path, result)

    print("")
    print("Done.")
    print(f"Task results written under: {task_results_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())