from lsmbench.benchmark.task_loader import (
    load_task_set,
    load_tasks_from_task_set,
)
from lsmbench.benchmark.task_registry import TaskRegistry


def test_load_pilot_task_set():
    task_set = load_task_set("pilot_v1")
    assert task_set["name"] == "pilot_v1"
    assert isinstance(task_set["tasks"], list)
    assert len(task_set["tasks"]) >= 10  # 6 GameBus + 4 public


def test_load_tasks_from_pilot():
    tasks = load_tasks_from_task_set("pilot_v1")
    task_ids = {task["task_id"] for task in tasks}

    # GameBus tasks
    assert "GB_001" in task_ids
    assert "GB_006" in task_ids

    # Public tasks
    assert "PUB_101" in task_ids
    assert "PUB_104" in task_ids


def test_registry_lookup():
    registry = TaskRegistry.from_task_set("pilot_v1")

    gb_task = registry.get("GB_001")
    pub_task = registry.get("PUB_101")

    assert gb_task["split"] == "gamebus"
    assert pub_task["split"] == "public"


def test_registry_filters():
    registry = TaskRegistry.from_task_set("pilot_v1")

    gamebus_tasks = registry.by_split("gamebus")
    public_tasks = registry.by_split("public")

    assert len(gamebus_tasks) >= 6
    assert len(public_tasks) >= 4

    easy_tasks = registry.by_difficulty("easy")
    assert len(easy_tasks) > 0