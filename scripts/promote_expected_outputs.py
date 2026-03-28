from __future__ import annotations

import argparse
import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Promote executed outputs into benchmark expected.sample.json files."
    )
    parser.add_argument(
        "task_ids",
        nargs="+",
        help="One or more task ids, e.g. GB_001 GB_004",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing expected.sample.json even if it is not a placeholder.",
    )
    return parser


def _load_json(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    for task_id in args.task_ids:
        executed_path = PROJECT_ROOT / "runs" / "reports" / f"{task_id}_executed_outputs.json"
        expected_path = PROJECT_ROOT / "benchmark" / "fixtures" / "gamebus" / f"{task_id}_expected.sample.json"

        if not executed_path.exists():
            print(f"[ERROR] Missing executed outputs: {executed_path}")
            continue

        executed = _load_json(executed_path)

        if expected_path.exists() and not args.force:
            existing = _load_json(expected_path)
            if not (isinstance(existing, dict) and existing.get("status") == "placeholder"):
                print(f"[SKIP] {task_id}: expected file already exists and is not a placeholder. Use --force to overwrite.")
                continue

        _write_json(expected_path, executed)
        print(f"[OK] Promoted {executed_path.name} -> {expected_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())