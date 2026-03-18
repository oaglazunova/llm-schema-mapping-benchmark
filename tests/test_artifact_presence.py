from pathlib import Path

def test_core_artifacts_exist():
    root = Path(__file__).resolve().parents[1]
    assert (root / "schemas" / "mapping_plan_v1.schema.json").exists()
    assert (root / "schemas" / "benchmark_task_v1.schema.json").exists()
    for tid in ["GB_001","GB_002","GB_003","GB_004","GB_005","GB_006"]:
        assert (root / "benchmark" / "tasks" / "gamebus" / f"{tid}_task.json").exists()
