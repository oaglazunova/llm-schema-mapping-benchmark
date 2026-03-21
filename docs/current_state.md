# Current State

This document summarizes the current implemented state of the benchmark.

## What the repository currently contains

The repository now contains a working benchmark prototype for **JSON-to-JSON LLM schema mapping** with:

- benchmark task JSON Schema
- mapping plan JSON Schema
- task loader and registry
- schema validation
- reference validation
- execution validation
- downstream validation
- grouped execution support
- minimal join execution support
- a task scaffolding helper for authoring new benchmark tasks

The benchmark is no longer only a collection of JSON files. It is a runnable and testable benchmark framework.

## Implemented benchmark angles

### 1. GameBus semi-real anchor
The benchmark includes hand-built GameBus-derived tasks based on sanitized fixture data.

Current GameBus tasks:
- `GB_001` — `NUTRITION_SUMMARY -> daily_nutrition_summary`
- `GB_002` — `DAY_AGGREGATE -> daily_activity_summary`
- `GB_003` — `NAVIGATE_APP -> navigation_event`
- `GB_004` — `CONSENT -> consent_record`
- `GB_005` — `GENERAL_SURVEY -> general_survey_profile`
- `GB_006` — `SCORE_GAMEBUS_POINTS -> points_event`
- `GB_007` — `NAVIGATE_APP[] -> navigation_session_summary`

These tasks cover:
- direct field mapping
- type coercion
- date normalization
- embedded JSON parsing
- key:value extraction
- grouped session rollups
- reward/event normalization

### 2. Public FHIR-to-GameBus interoperability angle
The benchmark includes public tasks that map FHIR-like resources into GameBus-shaped targets.

Current public FHIR tasks:
- `PUB_101` — `FHIR Patient -> gamebus_player_profile`
- `PUB_102` — `FHIR QuestionnaireResponse -> general_survey_profile`
- `PUB_103` — `FHIR Observation -> daily_activity_summary`
- `PUB_104` — `FHIR Observation -> daily_nutrition_summary`

This angle demonstrates:
- public reproducibility
- interoperability-oriented mapping
- nested JSON extraction
- compatibility between standards-based health data and application-shaped targets

### 3. Generic public JSON angle
The benchmark also includes non-healthcare public tasks.

Current public generic tasks:
- `PUB_105` — payment events -> `customer_payment_summary`
- `PUB_106` — joined order/customer bundles -> `order_with_customer`

This angle demonstrates that the benchmark is not only about lifestyle coaching or health standards. It also covers general JSON schemas from other domains.

## Execution model currently supported

The current execution engine supports four important benchmark shapes:

1. single-record transformation
2. filtered transformation
3. grouped/session rollup
4. minimal two-source join

This is enough to cover the current pilot benchmark tasks.

## Validation pipeline currently supported

Validation currently runs through these conceptual stages:

- task schema validation
- plan schema validation
- reference validation
- execution validation
- exact output validation
- invariant validation
- downstream validation

This means the benchmark can distinguish between:
- malformed plans
- reference-invalid plans
- executable but wrong plans
- correct plans

## What is not implemented yet

The current benchmark prototype does not yet support:

- multi-join chains
- joins combined with grouped execution
- full static semantic validation beyond current checks
- automatic public ingest pipeline
- automatic GameBus descriptor-to-task generation
- large synthetic split generation
- evaluation baselines over multiple LLMs

## Intended next phase

The next phase should focus on:
- semi-automated GameBus task generation
- broader public task coverage
- synthetic task generation
- baseline runners and evaluation reporting