# Roadmap

## Overview

FinStream is implemented incrementally so each data layer and component is working and testable before orchestration and operational tooling are added. This roadmap defines implementation order; future ideas are not commitments unless later requirements justify them.

## V1 Roadmap

### Step 0 — Project Foundation

**Scope:**

- Define project purpose and scope.
- Select the initial financial, market, and macroeconomic domains.
- Select the initial technology direction.

**Exit criteria:**

- The project purpose, initial data domains, and technology direction are defined.

### Step 1 — Repository and Documentation Foundation

**Scope:**

- Initialize the repository structure.
- Define project requirements and system architecture.
- Document external data sources and the logical data model.
- Define the implementation roadmap.
- Complete the README after the source-of-truth documentation is stable.

**Exit criteria:**

- Core documentation is internally consistent.
- Planned functionality is not presented as implemented.
- Implementation steps have clear boundaries.

### Step 2 — Python Project Foundation

**Scope:**

- Create the Python package structure and introduce dependency management.
- Add environment-driven configuration and shared logging utilities.
- Establish basic automated tests.

**Exit criteria:**

- The project can be installed and run in a clean local environment.
- Secrets remain outside Git.
- Basic test execution works.

### Step 3 — Market Data Ingestion

**Scope:**

- Implement Twelve Data integration for daily OHLCV data for configured companies.
- Validate responses and distinguish valid no-new-data results from provider failures.
- Persist source-aligned market data.

**Exit criteria:**

- Daily market data can be retrieved for the configured symbols.
- Source data is persisted reliably.
- Repeated retrieval does not create uncontrolled duplicates.

### Step 4 — SEC Financial Data Ingestion

**Scope:**

- Implement SEC request handling with an appropriate User-Agent.
- Retrieve filing metadata and structured XBRL company facts.
- Preserve filing and source identifiers required for traceability.
- Avoid prematurely forcing SEC concepts into canonical financial metrics.

**Exit criteria:**

- Financial source data can be retrieved for all initial companies.
- Filing and source-fact provenance remain traceable.

### Step 5 — FRED Macroeconomic Data Ingestion

**Scope:**

- Implement FRED series and observation retrieval.
- Support the configured daily, monthly, and quarterly series.
- Preserve metadata needed to understand source revisions.
- Handle valid missing values and no-new-data responses.

**Exit criteria:**

- All configured macroeconomic series can be ingested.
- Different observation frequencies do not break ingestion.

### Step 6 — Bronze Storage

**Scope:**

- Formalize raw JSON preservation where useful.
- Store structured raw datasets using Parquet.
- Define deterministic source-storage conventions.
- Preserve ingestion metadata for traceability and debugging.

**Exit criteria:**

- Retrieved source data can be reprocessed without immediately calling the provider again.
- Raw data remains traceable to its source and ingestion event.

### Step 7 — PostgreSQL and Warehouse Foundation

**Scope:**

- Introduce PostgreSQL.
- Create the PostgreSQL structures required for source-aligned loading.
- Load source-aligned data for downstream dbt transformation.
- Add constraints supporting integrity and duplicate prevention.

**Exit criteria:**

- Source-aligned data is stored reliably in PostgreSQL.
- Physical structures remain consistent with documented model grains.

### Step 8 — dbt Transformation Layer

**Scope:**

- Initialize dbt and implement Silver models and Gold dimensions and facts.
- Add model documentation, lineage, and core dbt tests.

**Exit criteria:**

- Source-aligned data can be transformed into documented and testable Silver and Gold analytical models.
- Silver and Gold grains match `DATA_MODEL.md`.

### Step 9 — Incremental Processing and Idempotency

**Status:** Complete.

**Scope:**

- Define incremental strategies and source-appropriate watermarks or retrieval boundaries.
- Implement safe upsert or merge behavior where appropriate.
- Handle reruns, partial failures, and source revisions where required.

**Exit criteria:**

- Reprocessing the same source range produces a stable result.
- New data can be processed without unnecessarily rebuilding all history.
- Duplicate analytical records are prevented.

### Step 10 — Data Quality and Automated Testing

**Status:** Complete.

**Scope:**

- Add Python tests for ingestion and validation behavior.
- Add dbt tests for documented model expectations and important cross-layer checks.
- Test failure, no-new-data, and repeated-run paths.

**Exit criteria:**

- Important source, transformation, and rerun behaviors are covered by automated tests.
- Critical data-quality rules are enforced.

### Step 11 — Airflow Orchestration

**Status:** In progress.

**Completed locally:**

- Step 11.1 establishes Apache Airflow as an optional, constraints-installed
  development/runtime dependency, reserves a non-package DAG directory, and
  defines orchestration, retry, and local WSL2 contracts.
- Step 11.2 adds Airflow-independent Market, SEC, and FRED source runtime
  adapters that compose the existing ingestion and PostgreSQL loading boundaries.
- Step 11.3 adds the manually triggered `finstream_v1_pipeline` DAG, with one
  visible source task per configured Market company, SEC company, and FRED
  series. Source tasks share the Airflow DAG run's `run_after` value as `run_at`.

dbt orchestration, data-quality orchestration, Airflow retry/task settings, and
production scheduling remain pending.

**Scope:**

- Introduce Apache Airflow.
- Coordinate ingestion, loading, transformation, and data-quality steps.
- Add retries and visible task failures.
- Keep transformation business logic outside DAG definitions.

**Exit criteria:**

- The complete V1 pipeline can execute in dependency order through Airflow.
- Individual components remain independently testable.

### Step 12 — Dockerized Local Environment

**Scope:**

- Containerize required local services.
- Provide a reproducible Docker Compose workflow.
- Keep credentials outside images and version control.

**Exit criteria:**

- Required local infrastructure can be started using documented commands.
- Another developer can reproduce the local environment.

### Step 13 — Analytics Marts

**Scope:**

- Implement approved analytical marts.
- Define mart grains, derived-metric calculations, and temporal-alignment rules.
- Initial logical marts may include `mart_company_daily_performance`, `mart_company_financial_growth`, and `mart_market_macro`.

**Exit criteria:**

- Financial, market, and macroeconomic data can be combined without ambiguous temporal alignment.
- Every implemented mart has a documented grain and calculation rules.

### Step 14 — Power BI

**Scope:**

- Connect Power BI to prepared analytical outputs.
- Build focused views for company, financial, market, and macroeconomic analysis.
- Keep shared transformation logic upstream when appropriate.

**Exit criteria:**

- Gold-layer outputs can be consumed successfully through Power BI.
- Dashboard logic does not unnecessarily duplicate shared transformation logic.

### Step 15 — CI, Observability, and Final Documentation

**Scope:**

- Add GitHub Actions for automated checks.
- Review logging and operational visibility.
- Complete setup and run documentation.
- Update the README with implemented architecture, features, screenshots, and usage.
- Clearly separate implemented functionality from future ideas.

**Exit criteria:**

- Automated checks run in CI.
- The repository can be understood and reproduced from its documentation.
- The README accurately represents the implemented system.

## Future Extensions

The following are outside V1:

- Higher-frequency market ingestion.
- Near-real-time processing and streaming architecture.
- Kafka and Apache Spark.
- Cloud object storage and a cloud data warehouse.
- Full macroeconomic vintage-history analytics.
- Broader company and economic-series coverage.

## Roadmap Boundaries

This document owns implementation order, step scope, and exit criteria.

It does not own project scope, requirements, goals, or non-goals (`PROJECT_REQUIREMENTS.md`); component responsibilities or system data flow (`ARCHITECTURE.md`); provider behavior or external-source constraints (`DATA_SOURCES.md`); or logical models, grains, identities, and analytical relationships (`DATA_MODEL.md`).

Detailed implementation decisions belong in the relevant implementation step and are not added prematurely to this roadmap.
