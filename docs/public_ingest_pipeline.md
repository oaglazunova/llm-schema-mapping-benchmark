# Public Ingest Pipeline

This benchmark has two ingestion modes.

## 1. Semi-automated private ingestion (GameBus)
Used for semi-real anchor tasks.

Pipeline:
1. Extract raw per-descriptor JSON from GameBus.
2. Profile descriptor structure.
3. Generate candidate benchmark task skeletons.
4. Manually sanitize, edit, and approve.
5. Publish only benchmark-safe task files, fixtures, and gold artifacts.

Rationale:
- preserves privacy
- leverages GameBus-specific semantics
- keeps a human review gate for benchmark quality

## 2. Fully automated public ingestion
Used later for public schema families such as FHIR and OpenAPI.

Pipeline:
1. Harvest source schemas/examples.
2. Normalize to benchmark source IR.
3. Match against target templates.
4. Generate candidate task files, fixtures, and gold skeletons.
5. Send all generated tasks through a human review gate before release.

## Why the two modes differ
GameBus already has a domain-aware extractor and descriptor structure, so a semi-automated pipeline is enough for v1.
Public sources are broader and noisier, so they benefit more from a generic ingest pipeline.