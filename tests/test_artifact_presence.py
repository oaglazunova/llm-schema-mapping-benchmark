from pathlib import Path

def test_core_artifacts_exist():
	root = Path(__file__).resolve().parents[1]
	assert (root / "schemas" / "mapping_plan_v1.schema.json").exists()
	assert (root / "schemas" / "benchmark_task_v1.schema.json").exists()
	for tid in ["GB_001","GB_002","GB_003","GB_004","GB_005","GB_006"]:
		assert (root / "benchmark" / "tasks" / "gamebus" / f"{tid}_task.json").exists()


def test_fhir_to_gamebus_public_artifacts_exist():
	root = Path(__file__).resolve().parents[1]

	for tid in ["PUB_101", "PUB_102", "PUB_103", "PUB_104"]:
		assert (root / "benchmark" / "tasks" / "public" / f"{tid}_task.json").exists()
		assert (root / "benchmark" / "fixtures" / "public" / f"{tid}_input.json").exists()
		assert (root / "benchmark" / "fixtures" / "public" / f"{tid}_expected.json").exists()
		assert (root / "benchmark" / "gold" / "matches" / f"{tid}_matches.json").exists()
		assert (root / "benchmark" / "gold" / "plans" / f"{tid}_plan.json").exists()
		assert (root / "benchmark" / "gold" / "invariants" / f"{tid}_invariants.json").exists()
