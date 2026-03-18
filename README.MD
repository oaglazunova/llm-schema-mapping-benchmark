Good. Then the next file to improve is:

```text
README.MD
```

This should stop being just a placeholder and become a **working entrypoint** for the repo.

You want the README to do four jobs:

1. explain **what this project is**
2. explain **what already works**
3. explain **how to run it**
4. explain **what is planned next**

---

# Put this into `README.MD`

````md
# LSMBench

**LSMBench** is a benchmark framework for **JSON-to-JSON LLM schema mapping**.

The current repository contains a first executable benchmark slice centered on **GameBus-derived tasks**. Each benchmark task includes:

- a task definition,
- a source schema,
- a target schema,
- fixture input,
- expected output,
- a gold mapping plan,
- invariants,
- validation scripts,
- regression tests.

The benchmark is designed to evaluate not only whether a mapping plan is structurally valid, but whether it is:

1. schema-valid,
2. reference-valid,
3. executable on sanitized/synthetic fixtures,
4. correct with respect to expected outputs and invariants.

---

## Current status

Implemented now:

- mapping plan JSON Schema
- benchmark task JSON Schema
- first GameBus anchor split:
  - `GB_001`
  - `GB_002`
  - `GB_003`
  - `GB_004`
  - `GB_005`
  - `GB_006`
- gold plans for all six tasks
- fixture-based validation pipeline
- batch validation script
- single-task validation script
- positive regression tests
- broken-plan regression tests

Planned next:

- public-anchor task split
- synthetic task generation
- richer join/filter semantics
- stronger validator modularization
- benchmark evaluation baselines

---

## Repository layout

```text
schemas/
  mapping_plan_v1.schema.json
  benchmark_task_v1.schema.json

benchmark/
  tasks/
    gamebus/
  fixtures/
    gamebus/
  gold/
    matches/
    plans/
    invariants/
  task_sets/

src/lsmbench/
  validators/
  execution/

scripts/
  validate_tasks.py
  validate_one_task.py

tests/
  test_validators.py
  test_broken_plans.py

docs/
  validator_design.md
````

---

## What the benchmark currently evaluates

The current benchmark slice focuses on **mapping plan validation and execution**.

Each task requires a plan to:

* conform to the plan schema,
* use valid source and target references,
* produce all required target fields,
* execute correctly on fixture input,
* match expected output,
* satisfy declared invariants.

This means the benchmark already distinguishes between:

* **invalid plans**
* **executable but wrong plans**
* **correct plans**

That distinction is central for LLM evaluation.

---

## Current GameBus anchor tasks

### GB_001 — NUTRITION_SUMMARY → `daily_nutrition_summary`

Tests:

* field copying
* integer casting
* date truncation

### GB_002 — DAY_AGGREGATE → `daily_activity_summary`

Tests:

* numeric casting
* simple daily summary normalization

### GB_003 — NAVIGATE_APP → `navigation_event`

Tests:

* event-level mapping
* datetime parsing
* URI preservation

### GB_004 — CONSENT → `consent_record`

Tests:

* embedded JSON parsing
* field remapping inside parsed arrays
* boolean aggregations
* invariant checking

### GB_005 — GENERAL_SURVEY → `general_survey_profile`

Tests:

* key:value extraction
* typed field derivation from semi-structured text

### GB_006 — SCORE_GAMEBUS_POINTS → `points_event`

Tests:

* numeric casting
* challenge / activity linkage preservation

---

## Supported plan operations in v1

### Field operations

* `copy`
* `rename`
* `cast_string`
* `cast_integer`
* `cast_number`
* `cast_boolean`
* `parse_date`
* `parse_datetime`
* `truncate_date`
* `normalize_enum`
* `normalize_boolean`
* `concat`
* `split`
* `derive_arithmetic`
* `default_value`
* `coalesce`
* `latest_value`
* `parse_json_array`
* `parse_json_array_map_fields`
* `parse_json_object`
* `parse_pythonish_object`
* `extract_kv_value`
* `extract_kv_value_cast_integer`
* `extract_object_field`
* `extract_array_field`

### Aggregation functions

* `count`
* `sum`
* `avg`
* `min`
* `max`
* `latest`
* `count_true`
* `all_true`
* `distinct_count`
* `first`

---

## Validation model

Validation currently runs in these stages:

* **V1** — plan schema validation
* **V2** — reference validation
* **V3** — static semantic validation
* **V4** — execution on fixtures
* **V5** — exact-output and invariant validation
* **V6** — downstream-check hook

See:

* `docs/validator_design.md`

for the detailed design.

---

## Quick start

### 1. Create and activate a virtual environment

#### PowerShell

```powershell
python -m venv .venv
. .venv/Scripts/Activate.ps1
```

### 2. Install dependencies

At minimum you need:

```powershell
pip install jsonschema pytest
```

If you later formalize dependencies in `pyproject.toml`, install with:

```powershell
pip install -e .
```

---

## Run the validator

### Validate all GameBus tasks

```powershell
python scripts/validate_tasks.py --show-passed
```

### Validate a single task

```powershell
python scripts/validate_one_task.py GB_004
```

### Inspect produced vs expected output for one task

```powershell
python scripts/validate_one_task.py GB_004 --show-produced --show-expected
```

### Inspect the task and gold plan

```powershell
python scripts/validate_one_task.py GB_004 --show-task --show-plan
```

---

## Run tests

### Run validator tests

```powershell
pytest tests/test_validators.py -q
```

### Run broken-plan tests

```powershell
pytest tests/test_broken_plans.py -q
```

### Run everything

```powershell
pytest -q
```

---

## What “passing” means right now

If a task passes validation, that currently means:

* the gold plan conforms to the schema,
* all source and target references are valid,
* required target fields are produced,
* the plan executes successfully on fixture input,
* produced output exactly matches expected output,
* invariants pass.

This is a strong benchmark guarantee for fixture-based evaluation.

It does **not** yet mean the plan is ready for unrestricted production use on arbitrary real data.

---

## Privacy and data handling

The benchmark artifacts are designed to be based on:

* sanitized inputs,
* synthetic or benchmark-safe fixtures,
* canonical target outputs,
* explicit task definitions.

The benchmark should **not** require releasing raw private user data.

GameBus-derived tasks in this repository are intended as a **semi-real anchor split**, not as a release of raw GameBus datasets.

---

## Why this benchmark exists

Most schema-mapping evaluation is still weakly standardized, and much current LLM work focuses more on **matching** than on **executable mapping plans**.

This project is aimed at a more rigorous evaluation setting where a mapping plan must:

* be well-formed,
* be executable,
* and be correct on benchmark fixtures.

That makes it more useful both for research evaluation and for safe system design.

---

## What is intentionally out of scope in v1

The current benchmark does **not** try to support:

* arbitrary SQL generation
* window functions
* regex extraction
* fuzzy matching
* recursive transforms
* free-form Python transforms
* ontology lookups

These are excluded on purpose so that the benchmark remains:

* deterministic,
* reproducible,
* and manageable for a first research release.

---

## Development roadmap

### Short-term

* stabilize the validator
* expand task documentation
* add public-anchor tasks
* improve benchmark metadata and task manifests

### Medium-term

* add synthetic task generation
* add benchmark baselines
* add richer task families
* modularize validator and execution code

### Longer-term

* publish a broader benchmark release
* evaluate multiple LLMs systematically
* compare prompt tracks and repair strategies
* add stronger semantic equivalence checking

---

## Recommended next files to read

If you are trying to understand the repo in order:

1. `README.MD`
2. `docs/validator_design.md`
3. `schemas/mapping_plan_v1.schema.json`
4. `benchmark/tasks/gamebus/GB_004_task.json`
5. `src/lsmbench/validators/pipeline.py`
6. `tests/test_validators.py`
7. `tests/test_broken_plans.py`

---

## Project state

This repository is currently a **working benchmark prototype**, not yet a final benchmark release.

The validator and first six tasks are real and executable.
The public-anchor and synthetic splits are still to be added.
