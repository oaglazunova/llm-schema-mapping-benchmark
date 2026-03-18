from __future__ import annotations

import ast
import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence

import jsonschema


# =========================================================
# Report model
# =========================================================

@dataclass
class ValidationIssue:
    stage: str
    level: str  # "error" | "warning" | "info"
    message: str
    path: Optional[str] = None


@dataclass
class ValidationReport:
    ok: bool = True
    issues: List[ValidationIssue] = field(default_factory=list)
    produced_output: Optional[List[Dict[str, Any]]] = None

    def add(self, stage: str, level: str, message: str, path: Optional[str] = None) -> None:
        self.issues.append(
            ValidationIssue(
                stage=stage,
                level=level,
                message=message,
                path=path,
            )
        )
        if level == "error":
            self.ok = False


# =========================================================
# File loading helpers
# =========================================================

def load_json(path: str | Path) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_task_bundle(task_path: str | Path) -> Dict[str, Any]:
    """
    Load:
      - task file
      - referenced fixture input
      - referenced expected output
      - referenced gold plan
      - referenced invariants

    Assumes task_path is something like:
      benchmark/tasks/gamebus/GB_001_task.json
    """
    task_path = Path(task_path).resolve()
    repo_root = task_path.parents[3]

    task = load_json(task_path)
    fixture_input = load_json(repo_root / task["fixture_refs"]["input"])
    fixture_expected = load_json(repo_root / task["fixture_refs"]["expected"])
    gold_plan = load_json(repo_root / task["gold_refs"]["plan"])
    invariants = load_json(repo_root / task["gold_refs"]["invariants"])

    return {
        "repo_root": repo_root,
        "task": task,
        "fixture_input": fixture_input,
        "fixture_expected": fixture_expected,
        "gold_plan": gold_plan,
        "invariants": invariants,
    }


# =========================================================
# JSON path helpers
# =========================================================

def _strip_root(path: str) -> str:
    if not path.startswith("$."):
        raise ValueError(f"Unsupported path syntax: {path}")
    return path[2:]


def get_value_by_path(obj: Any, path: str) -> Any:
    """
    Minimal JSONPath-like resolver for benchmark v1.

    Supports:
      $.field
      $.field.subfield
      $.field[*]
      $.field[*].subfield
    """
    path = _strip_root(path)

    # Case 1: terminal array selection, e.g. $.consent_items[*]
    if path.endswith("[*]"):
        left = path[:-3]
        value = obj.get(left)
        if isinstance(value, list):
            return value
        return []

    # Case 2: array projection, e.g. $.consent_items[*].accepted
    if "[*]." in path:
        left, right = path.split("[*].", 1)
        arr = obj.get(left, [])
        if not isinstance(arr, list):
            return []
        out = []
        for item in arr:
            if isinstance(item, dict) and right in item:
                out.append(item[right])
        return out

    # Case 3: plain dotted lookup
    parts = path.split(".")
    cur = obj
    for part in parts:
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur


def source_path_exists_in_schema(source_schema: Mapping[str, Any], path: str) -> bool:
    """
    Minimal schema-level path existence check.

    v1 assumption:
      - source schemas are mostly flat objects
      - if root field exists, we allow path
    """
    try:
        stripped = _strip_root(path)
    except ValueError:
        return False

    root = stripped.split(".", 1)[0].split("[", 1)[0]
    props = source_schema.get("properties", {})
    return isinstance(props, dict) and root in props


def target_field_exists_in_schema(target_schema: Mapping[str, Any], field_name: str) -> bool:
    props = target_schema.get("properties", {})
    return isinstance(props, dict) and field_name in props


# =========================================================
# Primitive coercions
# =========================================================

def coerce_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        s = value.strip()
        if s == "":
            return None
        return int(float(s))
    raise ValueError(f"Cannot coerce to int: {value!r}")


def coerce_number(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, str):
        s = value.strip()
        if s == "":
            return None
        return float(s)
    raise ValueError(f"Cannot coerce to number: {value!r}")


def coerce_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        s = value.strip().lower()
        if s in {"true", "1", "yes"}:
            return True
        if s in {"false", "0", "no"}:
            return False
    raise ValueError(f"Cannot coerce to bool: {value!r}")


def parse_datetime_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"Cannot parse datetime from non-string: {value!r}")
    dt = datetime.fromisoformat(value.replace(" ", "T"))
    return dt.isoformat(timespec="seconds")


def truncate_date_str(value: Any) -> Optional[str]:
    iso = parse_datetime_str(value)
    if iso is None:
        return None
    return iso[:10]


def parse_json_array_str(value: Any) -> List[Any]:
    if not isinstance(value, str):
        raise ValueError("parse_json_array expects a string")
    parsed = json.loads(value)
    if not isinstance(parsed, list):
        raise ValueError("Expected JSON array")
    return parsed


def parse_json_object_str(value: Any) -> Dict[str, Any]:
    if not isinstance(value, str):
        raise ValueError("parse_json_object expects a string")
    parsed = json.loads(value)
    if not isinstance(parsed, dict):
        raise ValueError("Expected JSON object")
    return parsed


def parse_pythonish_object_str(value: Any) -> Dict[str, Any]:
    """
    For values like:
      "{'src':'p','steps':15076,'distance':11253.33,'cals':534.90}"
    """
    if not isinstance(value, str):
        raise ValueError("parse_pythonish_object expects a string")
    parsed = ast.literal_eval(value)
    if not isinstance(parsed, dict):
        raise ValueError("Expected Python-like dict string")
    return parsed


def extract_kv_value(values: Sequence[str], key: str, delimiter: str = ":") -> Optional[str]:
    for item in values:
        if not isinstance(item, str):
            continue
        if delimiter not in item:
            continue
        k, v = item.split(delimiter, 1)
        if k.strip() == key:
            return v.strip()
    return None


# =========================================================
# Operations
# =========================================================

def apply_operation(
    operation: str,
    source_values: List[Any],
    parameters: Optional[Dict[str, Any]] = None,
) -> Any:
    parameters = parameters or {}

    if operation in {"copy", "rename", "latest_value"}:
        return source_values[0] if source_values else None

    if operation == "cast_string":
        value = source_values[0] if source_values else None
        return None if value is None else str(value)

    if operation == "cast_integer":
        return coerce_int(source_values[0] if source_values else None)

    if operation == "cast_number":
        return coerce_number(source_values[0] if source_values else None)

    if operation == "cast_boolean":
        return coerce_bool(source_values[0] if source_values else None)

    if operation == "parse_datetime":
        return parse_datetime_str(source_values[0] if source_values else None)

    if operation == "parse_date":
        return truncate_date_str(source_values[0] if source_values else None)

    if operation == "truncate_date":
        return truncate_date_str(source_values[0] if source_values else None)

    if operation == "default_value":
        return parameters.get("value")

    if operation == "coalesce":
        for val in source_values:
            if val is not None:
                return val
        return parameters.get("default")

    if operation == "concat":
        sep = parameters.get("separator", "")
        return sep.join("" if v is None else str(v) for v in source_values)

    if operation == "split":
        value = source_values[0] if source_values else None
        if value is None:
            return []
        sep = parameters.get("separator", ",")
        return str(value).split(sep)

    if operation == "normalize_enum":
        value = source_values[0] if source_values else None
        mapping = parameters.get("mapping", {})
        return mapping.get(value, value)

    if operation == "normalize_boolean":
        value = source_values[0] if source_values else None
        mapping = parameters.get("mapping", {})
        if value in mapping:
            return mapping[value]
        return coerce_bool(value)

    if operation == "derive_arithmetic":
        op = parameters.get("op")
        nums = [coerce_number(v) for v in source_values]
        if any(v is None for v in nums):
            return None
        if op == "add":
            return sum(nums)
        if op == "sub":
            return nums[0] - nums[1]
        if op == "mul":
            return nums[0] * nums[1]
        if op == "div":
            return None if nums[1] == 0 else nums[0] / nums[1]
        raise ValueError(f"Unsupported derive_arithmetic op: {op!r}")

    if operation == "parse_json_array":
        return parse_json_array_str(source_values[0] if source_values else None)

    if operation == "parse_json_array_map_fields":
        arr = parse_json_array_str(source_values[0] if source_values else None)
        field_map = parameters.get("field_map", {})
        if not isinstance(field_map, dict):
            raise ValueError("parse_json_array_map_fields requires 'field_map' dict")

        out = []
        for item in arr:
            if not isinstance(item, dict):
                raise ValueError("parse_json_array_map_fields expects array of objects")

            mapped = {}
            for src_key, value in item.items():
                tgt_key = field_map.get(src_key, src_key)
                mapped[tgt_key] = value
            out.append(mapped)
        return out

    if operation == "parse_json_object":
        return parse_json_object_str(source_values[0] if source_values else None)

    if operation == "parse_pythonish_object":
        return parse_pythonish_object_str(source_values[0] if source_values else None)

    if operation == "extract_kv_value":
        key = parameters["key"]
        delimiter = parameters.get("delimiter", ":")
        arr = source_values[0] if source_values else []
        if arr is None:
            return None
        if not isinstance(arr, list):
            raise ValueError("extract_kv_value expects a list of strings")
        return extract_kv_value(arr, key=key, delimiter=delimiter)

    if operation == "extract_kv_value_cast_integer":
        key = parameters["key"]
        delimiter = parameters.get("delimiter", ":")
        arr = source_values[0] if source_values else []
        if arr is None:
            return None
        if not isinstance(arr, list):
            raise ValueError("extract_kv_value_cast_integer expects a list of strings")
        value = extract_kv_value(arr, key=key, delimiter=delimiter)
        return coerce_int(value)

    if operation == "extract_object_field":
        field_name = parameters["field"]
        obj = source_values[0] if source_values else None
        if not isinstance(obj, dict):
            raise ValueError("extract_object_field expects an object")
        return obj.get(field_name)

    if operation == "extract_array_field":
        field_name = parameters["field"]
        arr = source_values[0] if source_values else []
        if not isinstance(arr, list):
            raise ValueError("extract_array_field expects a list")
        out = []
        for item in arr:
            if isinstance(item, dict) and field_name in item:
                out.append(item[field_name])
        return out

    raise ValueError(f"Unsupported operation: {operation}")


# =========================================================
# Aggregations
# =========================================================

def apply_aggregation(function: str, values: Any) -> Any:
    if function == "count":
        return len(values) if isinstance(values, list) else (0 if values is None else 1)

    if function == "count_true":
        if not isinstance(values, list):
            raise ValueError("count_true expects a list")
        return sum(1 for v in values if v is True)

    if function == "all_true":
        if not isinstance(values, list):
            raise ValueError("all_true expects a list")
        return all(v is True for v in values)

    if function == "distinct_count":
        if not isinstance(values, list):
            raise ValueError("distinct_count expects a list")
        return len(set(values))

    if function == "first":
        if not isinstance(values, list):
            raise ValueError("first expects a list")
        return values[0] if values else None

    if function == "latest":
        if not isinstance(values, list):
            raise ValueError("latest expects a list")
        return values[-1] if values else None

    if function == "sum":
        if not isinstance(values, list):
            raise ValueError("sum expects a list")
        return sum(coerce_number(v) or 0 for v in values)

    if function == "avg":
        if not isinstance(values, list):
            raise ValueError("avg expects a list")
        nums = [coerce_number(v) for v in values if v is not None]
        return None if not nums else sum(nums) / len(nums)

    if function == "min":
        if not isinstance(values, list):
            raise ValueError("min expects a list")
        nums = [coerce_number(v) for v in values if v is not None]
        return None if not nums else min(nums)

    if function == "max":
        if not isinstance(values, list):
            raise ValueError("max expects a list")
        nums = [coerce_number(v) for v in values if v is not None]
        return None if not nums else max(nums)

    raise ValueError(f"Unsupported aggregation function: {function}")


# =========================================================
# Static validation helpers
# =========================================================

PARAMETER_REQUIREMENTS = {
    "extract_kv_value": {"key"},
    "extract_kv_value_cast_integer": {"key"},
    "extract_object_field": {"field"},
    "extract_array_field": {"field"},
    "derive_arithmetic": {"op"},
    "parse_json_array_map_fields": {"field_map"},
}

def validate_plan_schema(plan: Dict[str, Any], schema: Dict[str, Any], report: ValidationReport) -> None:
    try:
        jsonschema.validate(plan, schema)
    except jsonschema.ValidationError as e:
        report.add(
            "V1_SCHEMA",
            "error",
            f"Plan does not conform to schema: {e.message}",
            path=str(list(e.path)),
        )


def validate_references(task: Dict[str, Any], plan: Dict[str, Any], report: ValidationReport) -> None:
    source_schema = task["source_schema"]
    target_schema = task["target_schema"]

    for i, fm in enumerate(plan.get("field_mappings", [])):
        target_field = fm["target_field"]
        if not target_field_exists_in_schema(target_schema, target_field):
            report.add(
                "V2_REFERENCES",
                "error",
                f"Unknown target field: {target_field}",
                path=f"field_mappings[{i}]",
            )

        for sp in fm.get("source_paths", []):
            if not source_path_exists_in_schema(source_schema, sp):
                report.add(
                    "V2_REFERENCES",
                    "error",
                    f"Unknown source path: {sp}",
                    path=f"field_mappings[{i}]",
                )

    # joins and filters are intentionally left minimal for v1


def validate_static_semantics(task: Dict[str, Any], plan: Dict[str, Any], report: ValidationReport) -> None:
    required_target_fields = set(task["target_schema"].get("required", []))
    covered_fields = {fm["target_field"] for fm in plan.get("field_mappings", [])}
    covered_fields.update(agg["target_field"] for agg in plan.get("aggregations", []))

    missing = sorted(required_target_fields - covered_fields)
    for field_name in missing:
        report.add(
            "V3_STATIC",
            "error",
            f"Required target field is not produced: {field_name}",
        )

    for i, fm in enumerate(plan.get("field_mappings", [])):
        op = fm["operation"]
        required_params = PARAMETER_REQUIREMENTS.get(op, set())
        params = fm.get("parameters", {}) or {}

        for param in required_params:
            if param not in params:
                report.add(
                    "V3_STATIC",
                    "error",
                    f"Operation '{op}' requires parameter '{param}'",
                    path=f"field_mappings[{i}]",
                )


# =========================================================
# Plan execution
# =========================================================

def execute_plan(
    task: Dict[str, Any],
    plan: Dict[str, Any],
    fixture_input: Dict[str, Any],
    report: ValidationReport,
) -> List[Dict[str, Any]]:
    records = fixture_input["records"]
    outputs: List[Dict[str, Any]] = []

    for rec_idx, record in enumerate(records):
        out: Dict[str, Any] = {}

        # 1. Field mappings
        for i, fm in enumerate(plan.get("field_mappings", [])):
            try:
                source_values = [
                    get_value_by_path(record, sp)
                    for sp in fm.get("source_paths", [])
                ]
                value = apply_operation(
                    fm["operation"],
                    source_values,
                    fm.get("parameters"),
                )
                out[fm["target_field"]] = value
            except Exception as e:
                report.add(
                    "V4_EXECUTION",
                    "error",
                    f"Failed to execute field mapping for target '{fm['target_field']}': {e}",
                    path=f"record[{rec_idx}].field_mappings[{i}]",
                )

        # 2. Aggregations over produced output
        for i, agg in enumerate(plan.get("aggregations", [])):
            try:
                if "source_path" in agg:
                    values = get_value_by_path(out, agg["source_path"])
                else:
                    values = []
                    for sp in agg["source_paths"]:
                        values.append(get_value_by_path(out, sp))

                agg_value = apply_aggregation(agg["function"], values)
                out[agg["target_field"]] = agg_value
            except Exception as e:
                report.add(
                    "V4_EXECUTION",
                    "error",
                    f"Failed to execute aggregation for target '{agg['target_field']}': {e}",
                    path=f"record[{rec_idx}].aggregations[{i}]",
                )

        outputs.append(out)

    return outputs


# =========================================================
# Exact output + invariants
# =========================================================

def validate_exact_output(
    expected: Dict[str, Any],
    produced: List[Dict[str, Any]],
    report: ValidationReport,
) -> None:
    expected_records = expected["records"]
    if produced != expected_records:
        report.add(
            "V5_OUTPUT",
            "error",
            "Produced output does not exactly match expected output.",
        )


def _eval_simple_formula(expr: str, record: Mapping[str, Any]) -> bool:
    """
    Very small safe formula evaluator for v1.
    Currently supports:
      field_a <= field_b
      field_a < field_b
      field_a >= field_b
      field_a > field_b
      field_a == field_b
    """
    supported_ops = ["<=", ">=", "==", "<", ">"]
    for op in supported_ops:
        if op in expr:
            left, right = expr.split(op, 1)
            left = left.strip()
            right = right.strip()
            lv = record.get(left)
            rv = record.get(right)

            if op == "<=":
                return lv <= rv
            if op == ">=":
                return lv >= rv
            if op == "==":
                return lv == rv
            if op == "<":
                return lv < rv
            if op == ">":
                return lv > rv

    raise ValueError(f"Unsupported formula expression: {expr}")


def validate_invariants(
    invariants: List[Dict[str, Any]],
    produced: List[Dict[str, Any]],
    report: ValidationReport,
) -> None:
    for rec_idx, rec in enumerate(produced):
        for inv in invariants:
            inv_type = inv["type"]
            field = inv.get("field")

            if inv_type == "range":
                val = rec.get(field)
                if val is None:
                    report.add("V5_INVARIANTS", "error", f"Missing field for range invariant: {field}")
                    continue
                if "min" in inv and val < inv["min"]:
                    report.add("V5_INVARIANTS", "error", f"{field}={val} violates min={inv['min']}")
                if "max" in inv and val > inv["max"]:
                    report.add("V5_INVARIANTS", "error", f"{field}={val} violates max={inv['max']}")

            elif inv_type == "prefix":
                val = rec.get(field)
                if not isinstance(val, str) or not val.startswith(inv["value"]):
                    report.add("V5_INVARIANTS", "error", f"{field} does not start with {inv['value']!r}")

            elif inv_type == "non_empty":
                val = rec.get(field)
                if val in (None, "", []):
                    report.add("V5_INVARIANTS", "error", f"{field} is empty")

            elif inv_type == "field_type":
                val = rec.get(field)
                expect = inv["expect"]
                ok = True

                if expect == "integer":
                    ok = isinstance(val, int)
                elif expect == "number":
                    ok = isinstance(val, (int, float))
                elif expect == "boolean":
                    ok = isinstance(val, bool)
                elif expect == "array":
                    ok = isinstance(val, list)
                elif expect == "datetime-string":
                    ok = isinstance(val, str) and "T" in val
                elif expect == "date-string":
                    ok = isinstance(val, str) and len(val) == 10 and val.count("-") == 2
                elif expect == "integer-or-null":
                    ok = val is None or isinstance(val, int)
                elif expect == "string-or-null":
                    ok = val is None or isinstance(val, str)

                if not ok:
                    report.add(
                        "V5_INVARIANTS",
                        "error",
                        f"{field} does not satisfy expected type {expect}",
                    )

            elif inv_type == "formula":
                try:
                    if not _eval_simple_formula(inv["expr"], rec):
                        report.add(
                            "V5_INVARIANTS",
                            "error",
                            f"Formula failed: {inv['expr']}",
                        )
                except Exception as e:
                    report.add(
                        "V5_INVARIANTS",
                        "error",
                        f"Formula evaluation failed: {inv['expr']} ({e})",
                    )

            else:
                report.add(
                    "V5_INVARIANTS",
                    "warning",
                    f"Unknown invariant type: {inv_type}",
                )


# =========================================================
# Downstream hooks
# =========================================================

def run_downstream_checks(
    produced: List[Dict[str, Any]],
    report: ValidationReport,
    downstream_checks: Optional[Sequence[Callable[[List[Dict[str, Any]]], Sequence[str]]]] = None,
) -> None:
    if not downstream_checks:
        return

    for i, check in enumerate(downstream_checks):
        try:
            messages = check(produced)
            for msg in messages:
                report.add("V6_DOWNSTREAM", "error", msg)
        except Exception as e:
            report.add(
                "V6_DOWNSTREAM",
                "error",
                f"Downstream check {i} failed with exception: {e}",
            )


# =========================================================
# Main entry points
# =========================================================

def validate_task_execution(
    task: Dict[str, Any],
    plan: Dict[str, Any],
    fixture_input: Dict[str, Any],
    fixture_expected: Dict[str, Any],
    invariants: List[Dict[str, Any]],
    mapping_plan_schema: Dict[str, Any],
    downstream_checks: Optional[Sequence[Callable[[List[Dict[str, Any]]], Sequence[str]]]] = None,
) -> ValidationReport:
    report = ValidationReport()

    validate_plan_schema(plan, mapping_plan_schema, report)
    if not report.ok:
        return report

    validate_references(task, plan, report)
    validate_static_semantics(task, plan, report)
    if not report.ok:
        return report

    produced = execute_plan(task, plan, fixture_input, report)
    report.produced_output = produced
    if not report.ok:
        return report

    validate_exact_output(fixture_expected, produced, report)
    validate_invariants(invariants, produced, report)
    run_downstream_checks(produced, report, downstream_checks)

    return report


def validate_task_file(
    task_path: str | Path,
    mapping_plan_schema_path: str | Path,
    downstream_checks: Optional[Sequence[Callable[[List[Dict[str, Any]]], Sequence[str]]]] = None,
) -> ValidationReport:
    bundle = load_task_bundle(task_path)
    report = validate_task_execution(
        task=bundle["task"],
        plan=bundle["gold_plan"],
        fixture_input=bundle["fixture_input"],
        fixture_expected=bundle["fixture_expected"],
        invariants=bundle["invariants"],
        mapping_plan_schema=load_json(mapping_plan_schema_path),
        downstream_checks=downstream_checks,
    )
    return report