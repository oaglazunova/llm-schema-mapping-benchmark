from __future__ import annotations

import ast
import json
import re
from collections import Counter
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Iterable

from lsmbench.config.paths import DESCRIPTOR_PROFILES_DIR, ensure_dir


INT_RE = re.compile(r"^-?\d+$")
FLOAT_RE = re.compile(r"^-?\d+(\.\d+)?$")
DATETIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}$")


@dataclass
class FieldProfile:
    field_name: str
    observed_count: int
    null_count: int
    raw_type_counts: dict[str, int]
    logical_type_hints: list[str]
    distinct_non_null_count: int
    example_values: list[Any]
    recommended_operations: list[str]
    enum_candidates: list[Any]


@dataclass
class DescriptorProfile:
    source_file: str
    descriptor_name: str
    top_level_type: str
    record_count: int
    field_count: int
    benchmark_family_hint: str
    field_profiles: list[FieldProfile]


def _infer_descriptor_name(path: Path, records: list[dict[str, Any]]) -> str:
    stem = path.stem.lower()
    if stem.startswith("player_"):
        parts = stem.split("_", 2)
        if len(parts) == 3:
            return parts[2]
    if records and isinstance(records[0], dict):
        if "gameDescriptor" in records[0]:
            return str(records[0]["gameDescriptor"])
    return stem


def _load_json_records(path: Path) -> tuple[str, list[dict[str, Any]]]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        records = [x for x in data if isinstance(x, dict)]
        return "list", records

    if isinstance(data, dict):
        # tolerate raw nested exports with object wrappers
        if "propertyInstances" in data:
            return "dict", [data]

        # try common wrappers
        for key in ("data", "items", "results", "records"):
            value = data.get(key)
            if isinstance(value, list) and all(isinstance(x, dict) for x in value):
                return "dict", value

        return "dict", [data]

    raise ValueError(f"Unsupported top-level JSON type in {path}: {type(data).__name__}")


def _raw_type_name(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int) and not isinstance(value, bool):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "str"
    if isinstance(value, list):
        return "list"
    if isinstance(value, dict):
        return "dict"
    return type(value).__name__


def _looks_like_datetime_string(value: str) -> bool:
    return bool(DATETIME_RE.match(value.strip()))


def _looks_like_int_string(value: str) -> bool:
    return bool(INT_RE.match(value.strip()))


def _looks_like_float_string(value: str) -> bool:
    s = value.strip()
    return bool(FLOAT_RE.match(s)) and "." in s


def _looks_like_json_array_string(value: str) -> bool:
    s = value.strip()
    if not (s.startswith("[") and s.endswith("]")):
        return False
    try:
        parsed = json.loads(s)
        return isinstance(parsed, list)
    except Exception:
        return False


def _looks_like_json_object_string(value: str) -> bool:
    s = value.strip()
    if not (s.startswith("{") and s.endswith("}")):
        return False
    try:
        parsed = json.loads(s)
        return isinstance(parsed, dict)
    except Exception:
        return False


def _looks_like_pythonish_object_string(value: str) -> bool:
    s = value.strip()
    if not (s.startswith("{") and s.endswith("}")):
        return False
    try:
        parsed = ast.literal_eval(s)
        return isinstance(parsed, dict)
    except Exception:
        return False


def _looks_like_kv_array(value: Any) -> bool:
    if not isinstance(value, list) or not value:
        return False
    str_items = [x for x in value if isinstance(x, str)]
    if len(str_items) != len(value):
        return False
    return all(":" in x for x in str_items)


def _logical_type_hints(values: list[Any]) -> list[str]:
    hints: set[str] = set()

    non_null = [v for v in values if v is not None]
    if not non_null:
        return []

    if all(isinstance(v, str) and _looks_like_datetime_string(v) for v in non_null):
        hints.add("datetime_string")

    if all(isinstance(v, str) and _looks_like_int_string(v) for v in non_null):
        hints.add("integer_string")

    if all(isinstance(v, str) and (_looks_like_int_string(v) or _looks_like_float_string(v)) for v in non_null):
        hints.add("numeric_string")

    if all(isinstance(v, str) and _looks_like_json_array_string(v) for v in non_null):
        hints.add("json_array_string")

    if all(isinstance(v, str) and _looks_like_json_object_string(v) for v in non_null):
        hints.add("json_object_string")

    if all(isinstance(v, str) and _looks_like_pythonish_object_string(v) for v in non_null):
        hints.add("pythonish_object_string")

    if all(_looks_like_kv_array(v) for v in non_null):
        hints.add("kv_array")

    if len(set(map(_safe_hashable, non_null))) <= 12 and all(not isinstance(v, (dict, list)) for v in non_null):
        hints.add("enum_like")

    return sorted(hints)


def _recommended_operations(field_name: str, hints: Iterable[str], raw_type_counts: Counter) -> list[str]:
    ops: list[str] = []

    hints = set(hints)

    if "datetime_string" in hints:
        if field_name.upper().endswith("DATE") or field_name.upper().startswith("X_DATE"):
            ops += ["parse_datetime", "truncate_date"]
        else:
            ops += ["parse_datetime"]

    if "integer_string" in hints:
        ops += ["cast_integer"]

    if "numeric_string" in hints and "integer_string" not in hints:
        ops += ["cast_number"]

    if "enum_like" in hints:
        ops += ["normalize_enum"]

    if "json_array_string" in hints:
        ops += ["parse_json_array"]

    if "json_object_string" in hints:
        ops += ["parse_json_object"]

    if "pythonish_object_string" in hints:
        ops += ["parse_pythonish_object"]

    if "kv_array" in hints:
        ops += ["extract_kv_value", "extract_kv_value_cast_integer"]

    if raw_type_counts.get("int", 0) > 0 or raw_type_counts.get("float", 0) > 0 or raw_type_counts.get("str", 0) > 0:
        ops = ["copy"] + ops

    # de-duplicate but preserve order
    out = []
    seen = set()
    for op in ops:
        if op not in seen:
            seen.add(op)
            out.append(op)
    return out


def _safe_hashable(value: Any) -> Any:
    if isinstance(value, list):
        return tuple(_safe_hashable(v) for v in value)
    if isinstance(value, dict):
        return tuple(sorted((k, _safe_hashable(v)) for k, v in value.items()))
    return value


def _profile_field(field_name: str, values: list[Any]) -> FieldProfile:
    raw_counter = Counter(_raw_type_name(v) for v in values)
    null_count = raw_counter.get("null", 0)
    non_null = [v for v in values if v is not None]

    examples: list[Any] = []
    seen = set()
    for v in non_null:
        hv = _safe_hashable(v)
        if hv in seen:
            continue
        seen.add(hv)
        examples.append(v)
        if len(examples) >= 5:
            break

    hints = _logical_type_hints(values)
    distinct_non_null_count = len({_safe_hashable(v) for v in non_null})
    enum_candidates: list[Any] = []

    if "enum_like" in hints:
        enum_candidates = examples[:10]

    return FieldProfile(
        field_name=field_name,
        observed_count=len(values),
        null_count=null_count,
        raw_type_counts=dict(raw_counter),
        logical_type_hints=hints,
        distinct_non_null_count=distinct_non_null_count,
        example_values=examples,
        recommended_operations=_recommended_operations(field_name, hints, raw_counter),
        enum_candidates=enum_candidates,
    )


def _benchmark_family_hint(descriptor_name: str, field_profiles: list[FieldProfile]) -> str:
    name = descriptor_name.lower()

    if "navigate" in name:
        return "event_log_or_session_rollup"
    if "consent" in name:
        return "embedded_json_parsing"
    if "survey" in name:
        return "kv_text_normalization"
    if "aggregate" in name or "summary" in name:
        return "flat_descriptor_normalization"
    if "tizen" in name:
        return "semi_structured_sensor_detail"

    hint_union = {hint for fp in field_profiles for hint in fp.logical_type_hints}
    if "json_array_string" in hint_union or "json_object_string" in hint_union:
        return "embedded_json_parsing"
    if "kv_array" in hint_union:
        return "kv_text_normalization"
    if "pythonish_object_string" in hint_union:
        return "semi_structured_sensor_detail"
    return "flat_descriptor_normalization"


def profile_descriptor_file(path: str | Path) -> DescriptorProfile:
    path = Path(path)
    top_level_type, records = _load_json_records(path)
    descriptor_name = _infer_descriptor_name(path, records)

    all_fields: set[str] = set()
    for rec in records:
        all_fields.update(rec.keys())

    field_profiles: list[FieldProfile] = []
    for field_name in sorted(all_fields):
        values = [rec.get(field_name) for rec in records]
        field_profiles.append(_profile_field(field_name, values))

    family = _benchmark_family_hint(descriptor_name, field_profiles)

    return DescriptorProfile(
        source_file=str(path),
        descriptor_name=descriptor_name,
        top_level_type=top_level_type,
        record_count=len(records),
        field_count=len(all_fields),
        benchmark_family_hint=family,
        field_profiles=field_profiles,
    )


def save_profile(profile: DescriptorProfile, output_path: str | Path | None = None) -> Path:
    ensure_dir(DESCRIPTOR_PROFILES_DIR)

    if output_path is None:
        safe_name = f"{profile.descriptor_name}.profile.json"
        output_path = DESCRIPTOR_PROFILES_DIR / safe_name
    else:
        output_path = Path(output_path)

    payload = asdict(profile)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    return output_path


def profile_many(paths: list[str | Path]) -> list[Path]:
    outputs: list[Path] = []
    for p in paths:
        prof = profile_descriptor_file(p)
        outputs.append(save_profile(prof))
    return outputs