from lsmbench.benchmark.task_loader import load_gold, load_task, load_fixture
from lsmbench.validators import validate_execution


def test_gb007_executes_successfully():
    task = load_task("benchmark/tasks/gamebus/GB_007_task.json")
    plan = load_gold(task, "plan")

    report = validate_execution(task, plan)

    assert report["valid"], report["errors"]