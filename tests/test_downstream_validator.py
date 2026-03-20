from lsmbench.benchmark.task_loader import load_gold, load_tasks_from_task_set
from lsmbench.validators import validate_downstream


def test_public_tasks_pass_downstream_validation():
    tasks = load_tasks_from_task_set("public_anchor_v1")
    failures = []

    for task in tasks:
        plan = load_gold(task, "plan")
        report = validate_downstream(task, plan)
        if not report["valid"]:
            failures.append((task["task_id"], report["errors"]))

    assert not failures, f"Downstream validation failures: {failures}"