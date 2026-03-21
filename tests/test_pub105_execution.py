from lsmbench.benchmark.task_loader import load_gold, load_task
from lsmbench.validators import validate_execution, validate_downstream


def test_pub105_executes_successfully():
    task = load_task("benchmark/tasks/public/PUB_105_task.json")
    plan = load_gold(task, "plan")

    report = validate_execution(task, plan)

    assert report["valid"], report["errors"]


def test_pub105_passes_downstream_validation():
    task = load_task("benchmark/tasks/public/PUB_105_task.json")
    plan = load_gold(task, "plan")

    report = validate_downstream(task, plan)

    assert report["valid"], report["errors"]