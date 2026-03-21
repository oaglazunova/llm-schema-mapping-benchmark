# Task Definition

A benchmark task is the basic evaluation unit of LSMBench.

Each task defines:

- a source schema
- a target schema
- a task description
- fixture input
- expected output
- gold field matches
- a gold mapping plan
- invariants
- optional downstream checks

## Task file

Each task is stored as a JSON file under:

- `benchmark/tasks/gamebus/`
- `benchmark/tasks/public/`
- later: `benchmark/tasks/synthetic/`

A task file contains:

- `task_id`
- `title`
- `split`
- `difficulty`
- `source_family`
- `target_entity`
- `task_text`
- `source_schema`
- `target_schema`
- `fixture_refs`
- `gold_refs`
- `tags`
- optional `downstream_checks`
- optional `notes`

## Fixtures

A task points to two fixture files:

- input fixture
- expected fixture

The current benchmark format uses:

```json
{
  "records": [...]
}
````

This allows:

* single-record tasks
* grouped tasks
* bundle-based join tasks

## Gold artifacts

Each task references three gold artifact files:

* `matches`
* `plan`
* `invariants`

### Gold matches

The match file documents intended source-to-target correspondences.

It is useful for:

* debugging
* schema matching evaluation
* explaining the task semantics

### Gold plan

The plan file is the benchmark’s main executable gold artifact.

It defines:

* field mappings
* joins
* filters
* aggregations
* optional grouping paths
* assumptions

### Invariants

Invariants define semantic constraints that should hold on the produced output.

They complement exact fixture matching and make task correctness more explicit.

## Task difficulty

Tasks are currently labeled as:

* `easy`
* `medium`
* `hard`

Difficulty is qualitative for now and should later be aligned with:

* number of fields
* nesting depth
* number of transformations
* grouping
* joins
* ambiguity

## Current task families

### GameBus task families

* nutrition summary
* day aggregate
* navigation event
* consent parsing
* general survey extraction
* points event
* navigation session rollup

### Public task families

* FHIR player/profile-style mapping
* FHIR survey-style mapping
* FHIR activity/nutrition summary mapping
* generic payment rollups
* generic order/customer join mapping

## Design principle

A task should be:

* specific enough to have a clear expected outcome
* rich enough to test a meaningful mapping capability
* small enough to be executable and debuggable
* stable enough to serve as a benchmark reference artifact
