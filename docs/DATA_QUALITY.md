# Data Quality

Quality means data can be represented, traced, and used at its documented
grain. A source revision, nullable value, or absence of new data is not
automatically invalid.

| Layer | Enforces |
| --- | --- |
| Python | Required provider shape, identities, types, and source-specific structural rules. |
| Bronze | Strict JSON, typed Parquet, deterministic counts, and artifact-pair verification. |
| PostgreSQL | Run provenance, keys, foreign keys, checks, and exact-run replay integrity. |
| dbt | Documented analytical grains, required fields, mappings, and relationships. |
| Monitoring | Read-only recency and run-count measurements. |

Source failures that violate required representation contracts block ingestion.
dbt test failures block analytical validation. Monitoring exposes `PASS`,
`WARNING`, `INFO`, and `ERROR` signals, but current recency and run-count
measurements are informational: no exchange calendar, publication cadence,
filing SLA, universal freshness threshold, or automated remediation is defined.

The status command reports an operational `ERROR` when a required query or
check cannot complete. An old source date alone remains `INFO`.
