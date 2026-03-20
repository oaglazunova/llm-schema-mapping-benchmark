from lsmbench.benchmark.task_loader import load_gold, load_tasks_from_task_set
from lsmbench.validators import validate_execution


def test_all_gold_plans_execute_on_pilot_fixtures():
    tasks = load_tasks_from_task_set("pilot_v1")
    failures = []

    for task in tasks:
        plan = load_gold(task, "plan")
        report = validate_execution(task, plan)
        if not report["valid"]:
            failures.append((task["task_id"], report["errors"]))

    assert not failures, f"Execution validation failures: {failures}"