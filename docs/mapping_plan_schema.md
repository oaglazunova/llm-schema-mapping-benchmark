# Mapping plan schema v1

The v1 plan language is intentionally minimal. It supports:

- direct field copy / rename
- type casting
- date parsing and truncation
- enum / boolean normalization
- embedded JSON parsing
- Python-like object parsing
- key:value extraction
- basic aggregations:
  - count
  - sum
  - avg
  - min
  - max
  - latest
  - count_true
  - all_true
  - distinct_count
  - first

The central goal is deterministic validation and execution over benchmark fixtures, not full ETL expressiveness.
