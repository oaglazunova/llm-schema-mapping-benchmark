from lsmbench.benchmark.task_loader import load_task_set


def test_synthetic_task_set_exists_and_has_tasks():
    task_set = load_task_set("synthetic_v1")
    assert task_set["name"] == "synthetic_v1"
    assert len(task_set["tasks"]) >= 8