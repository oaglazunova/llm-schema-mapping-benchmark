from __future__ import annotations

import ast
import json
import jsonschema

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence
from lsmbench.execution.engine import execute_plan_on_fixture as engine_execute_plan_on_fixture


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

from pathlib import Path
import json
from typing import Any, Dict


def load_json(path: str | Path) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _find_repo_root(start: Path) -> Path:
    cur = start.resolve()
    if cur.is_file():
        cur = cur.parent

    for candidate in [cur, *cur.parents]:
        if (candidate / "pyproject.toml").exists():
            return candidate

    raise RuntimeError(f"Could not locate repo root from {start}")


def _resolve_repo_relative(repo_root: Path, ref: str | Path) -> Path:
    p = Path(ref)
    if p.is_absolute():
        return p
    return repo_root / p


def load_task_bundle(task_path: str | Path) -> Dict[str, Any]:
    task_path = Path(task_path).resolve()
    task_dir = task_path.parent

    def _load(path: Path):
        return load_json(path)

    def _resolve_ref(ref: str) -> Path:
        ref_path = Path(ref)

        if ref_path.is_absolute():
            return ref_path

        # repo-root style refs
        if ref_path.parts and ref_path.parts[0] in {"benchmark", "schemas", "src", "tests", "docs"}:
            repo_root = _find_repo_root(task_path)
            return repo_root / ref_path

        # otherwise resolve relative to the task file
        return (task_dir / ref_path).resolve()

    task = _load(task_path)

    fixture_input = _load(_resolve_ref(task["fixture_refs"]["input"]))
    fixture_expected = _load(_resolve_ref(task["fixture_refs"]["expected"]))
    gold_plan = _load(_resolve_ref(task["gold_refs"]["plan"]))
    invariants = _load(_resolve_ref(task["gold_refs"]["invariants"]))

    return {
        "task": task,
        "fixture_input": fixture_input,
        "fixture_expected": fixture_expected,
        "gold_plan": gold_plan,
        "invariants": invariants,
    }


# =========================================================
# JSON path helpers
# =========================================================

def _fixture_to_records(obj: Any) -> list[dict[str, Any]]:
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict):
        if "records" in obj:
            return obj["records"]
        return [obj]
    raise ValueError(f"Unsupported fixture object type: {type(obj)!r}")


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


def _source_field_names(source_schema: Mapping[str, Any]) -> set[str]:
    # Profiled schema style
    if "fields" in source_schema and isinstance(source_schema["fields"], list):
        return {
            field["name"]
            for field in source_schema["fields"]
            if isinstance(field, dict) and "name" in field
        }

    # Plain JSON Schema style
    props = source_schema.get("properties", {})
    if isinstance(props, dict):
        return set(props.keys())

    return set()


def _target_field_names(target_schema: Mapping[str, Any]) -> set[str]:
    # Benchmark target schema style
    if "fields" in target_schema and isinstance(target_schema["fields"], list):
        return {
            field["name"]
            for field in target_schema["fields"]
            if isinstance(field, dict) and "name" in field
        }

    # Plain JSON Schema style
    props = target_schema.get("properties", {})
    if isinstance(props, dict):
        return set(props.keys())

    return set()


def source_path_exists_in_schema(source_schema: Mapping[str, Any], path: str) -> bool:
    """
    Minimal schema-level path existence check.

    Supported in v1:
      $.FIELD
      $.FIELD[*]
      $.FIELD[*].subfield   -> validated only against root field existence
      $.FIELD.subfield      -> validated only against root field existence
    """
    try:
        stripped = _strip_root(path)
    except ValueError:
        return False

    root = stripped.split(".", 1)[0].split("[", 1)[0]
    return root in _source_field_names(source_schema)


def target_field_exists_in_schema(target_schema: Mapping[str, Any], field_name: str) -> bool:
    return field_name in _target_field_names(target_schema)


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
    return dt.strftime("%Y-%m-%d %H:%M:%S")


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
    target_schema = task["target_schema"]

    if "fields" in target_schema and isinstance(target_schema["fields"], list):
        required_target_fields = {
            field["name"]
            for field in target_schema["fields"]
            if isinstance(field, dict) and field.get("required", False)
        }
    else:
        required_target_fields = set(target_schema.get("required", []))

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
    fixture_input: Any,
    report: ValidationReport,
) -> Optional[List[Dict[str, Any]]]:
    """
    Execute a mapping plan using the shared execution engine.

    This keeps validator-stage execution aligned with:
      - grouped execution
      - filters
      - joins
      - aggregation semantics
      - datetime normalization
    """
    try:
        produced_payload = engine_execute_plan_on_fixture(
            fixture_input,
            plan,
            task_payload=task,
        )

        if isinstance(produced_payload, dict) and "records" in produced_payload:
            produced = produced_payload["records"]
        elif isinstance(produced_payload, list):
            produced = produced_payload
        else:
            report.add(
                "V4_EXECUTION",
                "error",
                f"Execution returned unsupported payload type: {type(produced_payload)!r}",
            )
            return None

        if not isinstance(produced, list):
            report.add(
                "V4_EXECUTION",
                "error",
                f"Execution did not return a list of records: {type(produced)!r}",
            )
            return None

        return produced

    except Exception as e:
        report.add(
            "V4_EXECUTION",
            "error",
            f"Unhandled execution error: {e}",
        )
        return None


# =========================================================
# Exact output + invariants
# =========================================================

def validate_exact_output(
    expected: Dict[str, Any] | List[Dict[str, Any]],
    produced: List[Dict[str, Any]],
    report: ValidationReport,
) -> None:
    expected_records = _fixture_to_records(expected)
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
    invariants,
    produced: List[Dict[str, Any]],
    report: ValidationReport,
) -> None:
    """
    Supports both:
      1) structured invariants: list[dict]
      2) lightweight invariant files:
         {
           "task_id": "...",
           "invariants": ["human-readable text", ...]
         }

    Human-readable string invariants are currently documentation-only.
    """
    if invariants is None:
        return

    if isinstance(invariants, dict):
        raw = invariants.get("invariants", [])
    else:
        raw = invariants

    if not isinstance(raw, list):
        report.add(
            "V6_INVARIANTS",
            "error",
            f"Invariants payload must be a list or dict-with-invariants, got {type(invariants)!r}",
        )
        return

    structured = [inv for inv in raw if isinstance(inv, dict)]

    for rec_idx, rec in enumerate(produced):
        for inv in structured:
            inv_type = inv["type"]

            if inv_type == "field_type":
                field = inv["field"]
                expected = inv["expect"]
                value = rec.get(field)

                if expected == "integer":
                    ok = isinstance(value, int) and not isinstance(value, bool)
                elif expected == "number":
                    ok = isinstance(value, (int, float)) and not isinstance(value, bool)
                elif expected == "string":
                    ok = isinstance(value, str)
                elif expected == "boolean":
                    ok = isinstance(value, bool)
                elif expected == "array":
                    ok = isinstance(value, list)
                elif expected == "object":
                    ok = isinstance(value, dict)
                elif expected == "date-string":
                    ok = isinstance(value, str) and len(value) == 10 and value[4] == "-" and value[7] == "-"
                elif expected == "datetime-string":
                    ok = isinstance(value, str) and len(value) >= 19
                elif expected == "integer-or-null":
                    ok = value is None or (isinstance(value, int) and not isinstance(value, bool))
                elif expected == "string-or-null":
                    ok = value is None or isinstance(value, str)
                else:
                    ok = True

                if not ok:
                    report.add(
                        "V6_INVARIANTS",
                        "error",
                        f"Field {field!r} violates invariant type expectation {expected!r}",
                        path=f"produced[{rec_idx}]",
                    )

            elif inv_type == "range":
                field = inv["field"]
                value = rec.get(field)
                min_value = inv.get("min")
                max_value = inv.get("max")

                if value is None or not isinstance(value, (int, float)):
                    report.add(
                        "V6_INVARIANTS",
                        "error",
                        f"Field {field!r} is not numeric for range invariant",
                        path=f"produced[{rec_idx}]",
                    )
                    continue

                if min_value is not None and value < min_value:
                    report.add(
                        "V6_INVARIANTS",
                        "error",
                        f"Field {field!r} = {value!r} is below minimum {min_value!r}",
                        path=f"produced[{rec_idx}]",
                    )

                if max_value is not None and value > max_value:
                    report.add(
                        "V6_INVARIANTS",
                        "error",
                        f"Field {field!r} = {value!r} exceeds maximum {max_value!r}",
                        path=f"produced[{rec_idx}]",
                    )

            elif inv_type == "non_empty":
                field = inv["field"]
                value = rec.get(field)
                if value in (None, "", [], {}):
                    report.add(
                        "V6_INVARIANTS",
                        "error",
                        f"Field {field!r} must be non-empty",
                        path=f"produced[{rec_idx}]",
                    )

            elif inv_type == "prefix":
                field = inv["field"]
                prefix = inv["value"]
                value = rec.get(field)
                if not isinstance(value, str) or not value.startswith(prefix):
                    report.add(
                        "V6_INVARIANTS",
                        "error",
                        f"Field {field!r} must start with {prefix!r}",
                        path=f"produced[{rec_idx}]",
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