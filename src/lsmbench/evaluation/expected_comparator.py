from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any


@dataclass
class ComparisonIssue:
    level: str
    code: str
    message: str


@dataclass
class ComparisonReport:
    ok: bool
    issues: list[ComparisonIssue]


def _load_json(path: str | Path) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def compare_outputs(expected_path: str | Path, actual_path: str | Path) -> ComparisonReport:
    expected = _load_json(expected_path)
    actual = _load_json(actual_path)

    issues: list[ComparisonIssue] = []

    if isinstance(expected, dict) and expected.get("status") == "placeholder":
        issues.append(
            ComparisonIssue(
                level="error",
                code="placeholder_expected",
                message="Expected output file is still a placeholder.",
            )
        )
        return ComparisonReport(ok=False, issues=issues)

    if type(expected) is not type(actual):
        issues.append(
            ComparisonIssue(
                level="error",
                code="type_mismatch",
                message=f"Expected top-level type {type(expected).__name__}, got {type(actual).__name__}.",
            )
        )
        return ComparisonReport(ok=False, issues=issues)

    if expected != actual:
        issues.append(
            ComparisonIssue(
                level="error",
                code="output_mismatch",
                message="Actual outputs do not exactly match expected outputs.",
            )
        )
        return ComparisonReport(ok=False, issues=issues)

    return ComparisonReport(ok=True, issues=[])


def save_report(report: ComparisonReport, output_path: str | Path) -> Path:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "ok": report.ok,
        "issues": [asdict(issue) for issue in report.issues],
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    return output_path