# Validator design

This document describes the validation model for the JSON-to-JSON LLM schema-mapping benchmark.

The validator is not only a syntax checker. It is the mechanism that determines whether a mapping plan is:

1. structurally valid,
2. grounded in the task schema,
3. executable on benchmark fixtures,
4. correct with respect to benchmark expectations,
5. safe enough to be considered for later use on real data.

The benchmark therefore treats validation as a **multi-stage gate**, not a single yes/no schema check.

---

## 1. Purpose

The benchmark is designed for evaluating LLM-generated schema mapping plans.

A mapping plan can fail in several different ways:

- it may be malformed JSON,
- it may reference fields that do not exist,
- it may omit required outputs,
- it may execute but produce the wrong values,
- it may produce plausible output that still violates task-level invariants.

Because of that, the validator is intentionally designed to distinguish between:

- **invalid plans**, and
- **executable but wrong plans**.

This distinction is central to the benchmark.

---

## 2. Validation philosophy

The benchmark does **not** assume that a structurally valid plan is correct.

Instead, validation is staged:

- **static validation** checks whether a plan is well-formed and semantically grounded,
- **execution validation** checks whether the plan actually works on data,
- **output validation** checks whether the produced result matches benchmark expectations.

This reflects the intended benchmark safety model:

> A plan should only be considered safe enough for real data after it has passed schema checks, reference checks, execution checks, and downstream checks on non-real data.

The benchmark itself uses only sanitized or synthetic fixture data.

---

## 3. Validation stages

The validator currently has six stages.

### V1 — Plan schema validation

The plan is validated against:

- `schemas/mapping_plan_v1.schema.json`

This checks:
- required top-level fields,
- allowed structure,
- allowed operations,
- allowed aggregation functions,
- structural correctness of field mappings, joins, filters, and aggregations.

A plan that fails here is rejected immediately.

Typical failures:
- malformed JSON,
- missing `field_mappings`,
- unsupported operation name,
- invalid aggregation spec,
- wrong data type for a schema field.

---

### V2 — Reference validation

The validator checks whether the plan refers only to fields that exist in the task definition.

This stage uses:
- `task["source_schema"]`
- `task["target_schema"]`

It verifies:
- every `source_path` refers to an existing source field,
- every `target_field` exists in the target schema.

Typical failures:
- hallucinated source field,
- typo in target field,
- field reference copied from another task.

This stage is particularly important for LLM evaluation because field hallucination is a common failure mode.

---

### V3 — Static semantic validation

The validator checks whether the plan is semantically complete before execution.

Current checks include:
- all required target fields are produced,
- operation-specific parameters are present.

Examples:
- `extract_kv_value` requires `parameters.key`
- `extract_object_field` requires `parameters.field`
- `derive_arithmetic` requires `parameters.op`
- `parse_json_array_map_fields` requires `parameters.field_map`

This stage catches plans that are structurally valid JSON but still incomplete or ill-specified.

---

### V4 — Execution validation

The validator executes the plan on benchmark fixture input.

Execution currently supports:
- field mappings,
- primitive coercions,
- date/time parsing,
- JSON string parsing,
- Python-like object parsing,
- key:value extraction,
- field extraction from parsed objects/arrays,
- simple aggregations over produced output fields.

Execution uses only:
- sanitized fixture input,
- expected benchmark output,
- benchmark invariants.

No real user data is used during benchmark validation.

This stage rejects plans that:
- raise runtime errors,
- misuse an operation,
- fail to parse embedded structures,
- produce malformed intermediate output.

---

### V5 — Output validation

The produced output is validated against benchmark expectations.

This stage currently has two parts.

#### Exact output validation
The produced output must exactly match the expected fixture output.

This is the main validation mode for the current hand-built GameBus pilot tasks because it is:
- deterministic,
- easy to debug,
- and strict enough for a small gold benchmark.

#### Invariant validation
Additional correctness constraints are checked on the produced output.

Current invariant types:
- `range`
- `prefix`
- `non_empty`
- `field_type`
- `formula`

Examples:
- `points >= 1`
- `uri` starts with `/`
- `accepted_count <= item_count`

These invariants make the benchmark more robust and clarify correctness even when exact output alone would not fully explain the intended semantics.

---

### V6 — Downstream validation

The validator includes an optional downstream-check hook.

This stage is not heavily used in the pilot split yet, but it exists because benchmark plans are intended to model a stronger trust gate:

- first validate on fixture data,
- then optionally validate downstream compatibility,
- only after that consider whether a plan may be acceptable for use on real data.

Future downstream checks may include:
- analytics compatibility,
- feature pipeline compatibility,
- reporting compatibility,
- target-application schema readiness.

---

## 4. Why execution is part of validation

The benchmark treats execution as part of validation rather than only evaluation.

Why?

Because many plans can be:
- structurally valid,
- reference-valid,
- and still wrong.

For example:
- a numeric target field may use the wrong source field,
- an embedded JSON field may be parsed but not normalized,
- a summary may count the wrong list,
- a text field may be copied instead of canonically transformed.

Without execution and output checks, those plans would appear valid even though they are not correct for the task.

This is especially important in LLM benchmarking, where “plausible but wrong” outputs are common.

---

## 5. Supported field operations in v1

The current field operation set is intentionally minimal but sufficient for the first GameBus anchor tasks.

### Basic copy / coercion
- `copy`
- `rename`
- `cast_string`
- `cast_integer`
- `cast_number`
- `cast_boolean`

### Date / time
- `parse_date`
- `parse_datetime`
- `truncate_date`

### Normalization / composition
- `normalize_enum`
- `normalize_boolean`
- `concat`
- `split`
- `derive_arithmetic`
- `default_value`
- `coalesce`
- `latest_value`

### Semi-structured parsing
- `parse_json_array`
- `parse_json_array_map_fields`
- `parse_json_object`
- `parse_pythonish_object`

### Extraction
- `extract_kv_value`
- `extract_kv_value_cast_integer`
- `extract_object_field`
- `extract_array_field`

These operations were chosen to support realistic benchmark patterns already present in the GameBus pilot tasks:
- flattened event mappings,
- daily summaries,
- embedded consent JSON,
- key:value survey parsing,
- normalized points events.

---

## 6. Supported aggregation functions in v1

Current aggregation functions:

- `count`
- `sum`
- `avg`
- `min`
- `max`
- `latest`
- `count_true`
- `all_true`
- `distinct_count`
- `first`

These are sufficient for:
- summary counts,
- boolean consent summaries,
- event/session rollups,
- simple numeric summaries.

The benchmark deliberately does **not** support arbitrary SQL aggregation or custom code in v1.

---

## 7. What the validator currently guarantees

If a task passes the current validator, that means:

1. the mapping plan matches the benchmark plan schema,
2. source and target references are grounded in the task definition,
3. required target fields are produced,
4. required operation parameters are present,
5. the plan executes on benchmark fixture input,
6. the produced output matches the expected output,
7. all benchmark invariants pass.

This is a strong guarantee for benchmark purposes.

It is **not** yet a guarantee that the plan is safe for unrestricted production use on arbitrary real datasets.

---

## 8. What the validator does not yet guarantee

The current validator is intentionally limited.

It does **not** yet fully guarantee:
- deep join correctness,
- full filter semantics,
- arbitrary nested-path schema reasoning,
- multi-record pipeline semantics beyond the current task style,
- semantic equivalence across multiple alternative valid plans.

It also does not support:
- arbitrary SQL expressions,
- regex extraction,
- fuzzy matching,
- recursion,
- window functions,
- free-form Python transforms,
- ontology lookups.

These exclusions are deliberate. The goal of v1 is deterministic reproducibility, not full ETL expressiveness.

---

## 9. Why broken-plan tests matter

The benchmark does not only test that gold plans pass.

It also includes broken-plan regression tests to prove that the validator rejects bad plans for meaningful reasons.

Examples of intentionally broken plans:
- hallucinated source field,
- missing required target output,
- missing required operation parameter,
- executable but wrong mapping,
- non-canonical normalization.

This is important because a benchmark validator should demonstrate both:
- **true positives**: valid plans pass,
- **true negatives**: invalid or semantically wrong plans fail.

Without broken-plan tests, validator quality is much harder to trust.

---

## 10. Design rationale for the current architecture

The implementation is currently centered in:

- `src/lsmbench/validators/pipeline.py`

This is intentional.

Although the repo has placeholder files for more modular validators, the benchmark currently prioritizes:

- one working end-to-end validation path,
- deterministic behavior,
- easy debugging,
- stable regression testing.

The current implementation can be refactored later into:
- `schema_validator.py`
- `reference_validator.py`
- `static_semantics.py`
- `execution_validator.py`
- `downstream_validator.py`

but only after behavior is stable.

---

## 11. Current implementation status

Implemented:
- schema validation
- reference validation
- static semantic validation
- execution validation
- exact output validation
- invariant validation
- downstream hook
- batch validation script
- single-task validation script
- gold-plan regression tests
- broken-plan regression tests

Current pilot task split:
- GB_001
- GB_002
- GB_003
- GB_004
- GB_005
- GB_006

These tasks now form an executable benchmark anchor split.

---

## 12. Task-specific examples

The current validator design was directly shaped by the pilot tasks.

Examples:

### GB_004 — CONSENT
This task requires:
- parsing a JSON string,
- mapping internal field names into canonical benchmark field names,
- aggregating boolean acceptance values,
- checking exact output and invariants.

This is the strongest example of why “schema-valid” is not enough.

### GB_005 — GENERAL_SURVEY
This task requires:
- parsing `key:value` strings,
- extracting typed values,
- preserving the original raw list.

This motivated the `extract_kv_value` operations.

### GB_003 — NAVIGATE_APP
This task requires:
- datetime parsing,
- correct URI preservation,
- exact event-level mapping.

This is a good example of an executable but semantically wrong plan if the wrong field is mapped.

---

## 13. Safety model

The intended benchmark safety model is:

1. validate structure,
2. validate references,
3. validate static semantics,
4. execute on sanitized/synthetic fixtures,
5. validate output and invariants,
6. optionally validate downstream compatibility,
7. only then consider whether a plan is acceptable for real data.

The benchmark itself stops at fixture-based validation.

Any later execution on real data belongs to a stricter deployment environment outside the benchmark.

---

## 14. Related files

Main implementation files:
- `src/lsmbench/validators/pipeline.py`
- `scripts/validate_tasks.py`
- `scripts/validate_one_task.py`

Related tests:
- `tests/test_validators.py`
- `tests/test_broken_plans.py`

Related schemas:
- `schemas/mapping_plan_v1.schema.json`
- `schemas/benchmark_task_v1.schema.json`

Related benchmark artifacts:
- `benchmark/tasks/gamebus/`
- `benchmark/fixtures/gamebus/`
- `benchmark/gold/`

---

## 15. Future extensions

The validator is expected to grow in three directions.

### Public-anchor tasks
Add public benchmark tasks in the same artifact format and validate them with the same pipeline.

### Synthetic tasks
Add generated tasks later while keeping the same validation contract.

### Stronger semantics
Extend validation to better support:
- joins,
- filters,
- more expressive task families,
- alternative but equivalent valid plans.

The current validator is intentionally minimal, but it already provides a solid benchmark kernel.