# LLM Schema Mapping Benchmark

A benchmark framework for **JSON-to-JSON schema mapping with LLMs**, focused on:
- **semi-real anchor tasks** derived from GameBus descriptor exports,
- **structured mapping plans** as the target output format,
- **deterministic validation and execution** on sample/sanitized fixtures,
- and a path toward **public anchor tasks** and later **synthetic generation**.

## Current status

The repository currently includes a fully working **GameBus semi-real anchor split**:

- `gamebus_anchor_v1`
- **7 / 7 tasks passing**
- mixed evaluation modes:
  - **exact expected-output comparison**
  - **invariant-based checking**

Implemented pipeline:
1. profile raw GameBus descriptor JSON files
2. scaffold benchmark tasks from descriptor profiles
3. validate task and gold-plan artifacts against schemas
4. execute gold plans on sample fixtures
5. evaluate outputs with exact comparison or invariants
6. run the whole anchor split as one task set

## Benchmark design

Each benchmark task contains:
- a **source schema**
- a **target schema**
- a **task description**
- **gold matches**
- a **gold mapping plan**
- **input fixtures**
- **expected outputs** or **invariants**

The benchmark uses a **structured JSON mapping plan** rather than free-text answers.

### Evaluation modes

Two evaluation modes are supported:

- **exact**  
  Executed outputs must exactly match the expected output fixture.

- **invariants**  
  Executed outputs must satisfy semantic checks when exact output matching is less appropriate.

## Repository structure

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
    gamebus_anchor_v1.json

data/
  raw_private/
    gamebus/
  interim_private/
    descriptor_profiles/

src/lsmbench/
  config/
  sources/gamebus/
  validators/
  execution/
  evaluation/

scripts/
  profile_gamebus_descriptor.py
  build_gamebus_tasks.py
  validate_gamebus_tasks.py
  execute_gamebus_gold_plan.py
  promote_expected_outputs.py
  compare_expected_outputs.py
  check_gamebus_invariants.py
  run_gamebus_anchor_v1.py
````

## GameBus anchor split

### Included tasks

* `GB_001` — `NUTRITION_SUMMARY -> daily_nutrition_summary`
* `GB_002` — `DAY_AGGREGATE -> daily_activity_summary`
* `GB_003` — `NAVIGATE_APP -> navigation_event`
* `GB_004` — `CONSENT -> consent_record`
* `GB_005` — `GENERAL_SURVEY -> general_survey_profile`
* `GB_006` — `SCORE_GAMEBUS_POINTS -> points_event`
* `GB_007` — `TIZEN(detail) -> sensor_activity_detail`

### Current task-set result

`gamebus_anchor_v1` currently passes fully:

* passed: **7 / 7**
* pending: **0**
* failed: **0**

Reports are written to:

```text
runs/reports/
```

## How to run

### 1. Profile raw GameBus descriptor files

```powershell
python .\scripts\profile_gamebus_descriptor.py `
  .\data\raw_private\gamebus\player_451_nutrition_summary.json `
  .\data\raw_private\gamebus\player_502_navigate_app.json `
  .\data\raw_private\gamebus\player_502_consent.json `
  .\data\raw_private\gamebus\player_502_general_survey.json `
  .\data\raw_private\gamebus\player_502_score_gamebus_points.json `
  .\data\raw_private\gamebus\player_449_day_aggregate.json `
  ".\data\raw_private\gamebus\player_449_tizen(detail).json"
```

This writes descriptor profiles under:

```text
data/interim_private/descriptor_profiles/
```

### 2. Build GameBus task scaffolds

```powershell
python .\scripts\build_gamebus_tasks.py `
  .\data\interim_private\descriptor_profiles\nutrition_summary.profile.json `
  .\data\interim_private\descriptor_profiles\navigate_app.profile.json `
  .\data\interim_private\descriptor_profiles\consent.profile.json `
  .\data\interim_private\descriptor_profiles\general_survey.profile.json `
  .\data\interim_private\descriptor_profiles\score_gamebus_points.profile.json `
  .\data\interim_private\descriptor_profiles\day_aggregate.profile.json `
  ".\data\interim_private\descriptor_profiles\tizen(detail).profile.json"
```

### 3. Validate selected task bundles

```powershell
python .\scripts\validate_gamebus_tasks.py GB_001 GB_004 GB_007
```

### 4. Execute gold plans on sample fixtures

```powershell
python .\scripts\execute_gamebus_gold_plan.py GB_001 GB_004 GB_007
```

### 5. Promote executed outputs to expected outputs

```powershell
python .\scripts\promote_expected_outputs.py GB_001 GB_004
```

### 6. Compare exact expected outputs

```powershell
python .\scripts\compare_expected_outputs.py GB_001 GB_004
```

### 7. Check invariants

```powershell
python .\scripts\check_gamebus_invariants.py GB_005 GB_007
```

### 8. Run the full GameBus anchor benchmark

```powershell
python .\scripts\run_gamebus_anchor_v1.py
```

This writes a summary to:

```text
runs/reports/gamebus_anchor_v1_summary.json
runs/reports/gamebus_anchor_v1_summary.csv
```

## Mapping-plan language

The benchmark uses a constrained operation language for mapping plans, including:

* copy / rename
* type casts
* date and datetime parsing
* enum normalization
* JSON-array / JSON-object parsing
* Python-like object parsing
* key:value extraction
* simple aggregations such as:

  * `count`
  * `sum`
  * `latest`
  * `count_true`
  * `all_true`
  * `distinct_count`
  * `first`

This language is designed to be:

* expressive enough for realistic JSON mapping tasks,
* but still small enough for deterministic validation and execution.

## Privacy and release model

This repository distinguishes between:

* **private raw source data**
  Stored under `data/raw_private/`

* **benchmark artifacts**
  Stored under `benchmark/`

GameBus-derived tasks are intended to be released only in **sanitized benchmark form**:

* source schema summaries
* sample/sanitized fixtures
* gold plans
* invariants
* benchmark metadata

Raw GameBus user data should not be released.


## Reporting

Run benchmark reporting for a task set:

```bash
python scripts/report_task_set.py --task-set pilot_v1
````

This writes:

* `runs/reports/<task_set>_summary.json`
* `runs/reports/<task_set>_summary.csv`

The JSON contains:

* aggregate validity rates
* per-task detailed reports
* split-level and difficulty-level summaries

The CSV contains:

* one row per task
* pass/fail columns for task, plan, reference, execution, downstream, and overall validity

