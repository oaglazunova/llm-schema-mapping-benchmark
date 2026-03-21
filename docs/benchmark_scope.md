# Benchmark Scope

## Benchmark goal

This benchmark evaluates **JSON-to-JSON schema mapping plans** produced by LLMs.

The core benchmark question is not only:

> “Can a model propose plausible correspondences?”

but also:

> “Can a model produce a mapping plan that is structurally valid, grounded in the task schema, executable on data, and correct with respect to target expectations?”

The benchmark is therefore designed around **mapping plans**, not only schema matching.

## In scope

The current benchmark scope includes:

- JSON-to-JSON mapping tasks
- source schemas represented as JSON objects or bundles
- target schemas represented as canonical JSON objects
- deterministic mapping plans in a restricted operation language
- validation against fixture input and expected output
- invariants and downstream checks
- semi-real tasks
- public interoperability tasks
- public generic JSON tasks

## Current benchmark angles

### 1. GameBus semi-real angle
A semi-real anchor split derived from sanitized GameBus data.

Purpose:
- application realism
- descriptor-based mapping patterns
- behavior/engagement/wellbeing data structures

### 2. FHIR-to-GameBus interoperability angle
A public angle mapping standards-based FHIR resources into GameBus-shaped targets.

Purpose:
- public reproducibility
- interoperability framing
- nested standards-based JSON

### 3. Generic public JSON angle
A public angle using non-healthcare JSON task families.

Purpose:
- show the benchmark is not limited to lifestyle coaching
- cover grouped and join tasks in generic domains

## Mapping patterns currently covered

The benchmark currently covers:

- direct field mapping
- renaming
- type coercion
- date and datetime normalization
- enum normalization
- embedded JSON parsing
- Python-like object parsing
- key:value extraction
- grouped rollups
- minimal joins
- simple aggregations
- filters

## Explicitly out of scope for the current version

The current benchmark version does not attempt to cover:

- arbitrary SQL generation
- XML mapping
- ontology mapping
- recursive transforms
- fuzzy joins
- free-form code generation
- complex workflow orchestration
- unbounded multi-hop relational integration

These may be explored later, but they are not part of the current benchmark contract.

## Intended benchmark progression

The intended progression is:

1. curated hand-built anchor tasks
2. public benchmark angle expansion
3. semi-automated task generation
4. synthetic controlled expansion
5. multi-model evaluation at scale

This ordering keeps the benchmark scientifically grounded while staying realistic for a solo implementation effort.