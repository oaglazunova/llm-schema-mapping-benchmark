from pathlib import Path

from lsmbench.validators.pipeline import validate_task_file


def main() -> int:
    project_root = Path(__file__).resolve().parents[2]
    report = validate_task_file(
        task_path=project_root / "benchmark" / "tasks" / "gamebus" / "GB_004_task.json",
        mapping_plan_schema_path=project_root / "schemas" / "mapping_plan_v1.schema.json",
    )

    print("OK:", report.ok)
    for issue in report.issues:
        print(f"[{issue.stage}] {issue.level.upper()}: {issue.message}")
    print("Produced output:", report.produced_output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())