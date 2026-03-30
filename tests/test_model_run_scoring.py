from __future__ import annotations

import json
from pathlib import Path

from lsmbench.benchmark.task_loader import load_task, load_gold
from lsmbench.evaluation.model_run_scoring import score_run_directory


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _tiny_task_set(tmp_path: Path) -> Path:
    task_set_path = tmp_path / "tiny_task_set.json"
    payload = {
        "name": "tiny_task_set",
        "version": "0.1.0",
        "tasks": [
            "benchmark/tasks/public/PUB_101_task.json"
        ]
    }
    _write_json(task_set_path, payload)
    return task_set_path


def test_score_run_directory_with_valid_result(tmp_path: Path):
    task = load_task("benchmark/tasks/public/PUB_101_task.json")
    gold_plan = load_gold(task, "plan")

    task_set_path = _tiny_task_set(tmp_path)
    run_dir = tmp_path / "runs" / "dummy-model" / "tiny_task_set" / "run_001"

    _write_json(
        run_dir / "task_results" / "PUB_101.json",
        {
            "schema_version": "model_run_record_v1",
            "run_id": "run_001",
            "task_set_id": "tiny_task_set",
            "task_id": "PUB_101",
            "model": {"model_name": "dummy-model"},
            "status": "ok",
            "selected_candidate_id": "c1",
            "candidates": [
                {
                    "candidate_id": "c1",
                    "raw_text": "{}",
                    "candidate_plan": gold_plan,
                    "parse_error": None
                }
            ],
            "usage": {
                "input_tokens": 10,
                "output_tokens": 20,
                "total_tokens": 30,
                "latency_ms": 50.0,
                "cost_usd": 0.001
            }
        },
    )

    report = score_run_directory(run_dir, task_set_path)

    assert report["summary"]["task_count"] == 1
    assert report["summary"]["overall_valid_rate"] == 1.0
    assert report["rows"][0]["overall_valid"] is True


def test_score_run_directory_with_missing_result(tmp_path: Path):
    task_set_path = _tiny_task_set(tmp_path)
    run_dir = tmp_path / "runs" / "dummy-model" / "tiny_task_set" / "run_002"
    run_dir.mkdir(parents=True, exist_ok=True)

    report = score_run_directory(run_dir, task_set_path)

    assert report["summary"]["task_count"] == 1
    assert report["summary"]["overall_valid_rate"] == 0.0
    assert report["rows"][0]["result_present"] is False
    assert "missing_result" in report["rows"][0]["error_codes"]