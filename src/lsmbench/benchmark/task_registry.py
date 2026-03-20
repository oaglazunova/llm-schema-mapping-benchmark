from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from lsmbench.benchmark.task_loader import load_all_tasks, load_tasks_from_task_set


@dataclass
class TaskRegistry:
    tasks_by_id: dict[str, dict[str, Any]]

    @classmethod
    def from_tasks(cls, tasks: list[dict[str, Any]]) -> "TaskRegistry":
        tasks_by_id: dict[str, dict[str, Any]] = {}
        for task in tasks:
            task_id = task["task_id"]
            if task_id in tasks_by_id:
                raise ValueError(f"Duplicate task_id in registry: {task_id}")
            tasks_by_id[task_id] = task
        return cls(tasks_by_id=tasks_by_id)

    @classmethod
    def from_task_set(cls, task_set: str) -> "TaskRegistry":
        return cls.from_tasks(load_tasks_from_task_set(task_set))

    @classmethod
    def from_all_tasks(cls, split: str | None = None) -> "TaskRegistry":
        return cls.from_tasks(load_all_tasks(split=split))

    def get(self, task_id: str) -> dict[str, Any]:
        try:
            return self.tasks_by_id[task_id]
        except KeyError as e:
            raise KeyError(f"Unknown task_id: {task_id}") from e

    def list_ids(self) -> list[str]:
        return sorted(self.tasks_by_id.keys())

    def list_tasks(self) -> list[dict[str, Any]]:
        return [self.tasks_by_id[tid] for tid in self.list_ids()]

    def by_split(self, split: str) -> list[dict[str, Any]]:
        return [task for task in self.list_tasks() if task["split"] == split]

    def by_difficulty(self, difficulty: str) -> list[dict[str, Any]]:
        return [task for task in self.list_tasks() if task["difficulty"] == difficulty]

    def by_tag(self, tag: str) -> list[dict[str, Any]]:
        return [task for task in self.list_tasks() if tag in task.get("tags", [])]