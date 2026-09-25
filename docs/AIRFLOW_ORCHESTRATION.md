# Airflow Orchestration Contract

## Ownership and Dependency Direction

Airflow owns workflow orchestration and dependency ordering. The intended stage
order is:

```text
source ingestion
    -> Bronze persistence
    -> PostgreSQL source loading
    -> dbt transformation
    -> data-quality evaluation
```

Airflow does not own provider-specific source semantics, source validation,
Bronze storage semantics, incremental watermark logic, replay or idempotency
rules, PostgreSQL source-loading behavior, dbt transformation business logic, or
data-quality policy. Those responsibilities remain in their existing FinStream
components, which must remain independently callable and testable without
Airflow.

## Source Runtime Adapter Boundary

The Airflow-independent `finstream.orchestration` package provides the callable
source boundaries for Market, SEC, and FRED:

- `run_market_source(symbol, *, run_at, settings=None, bronze_root=...)`
- `run_sec_source(cik, *, run_at, settings=None, bronze_root=...)`
- `run_fred_source(series_id, *, run_at, settings=None, bronze_root=...)`

Each adapter composes existing FinStream ingestion and PostgreSQL-loading
components rather than reimplementing source logic. Market and FRED use their
existing incremental Bronze ingestion boundaries; SEC uses its existing complete
company Bronze ingestion boundary. Each adapter owns one `connect_postgres()`
connection context and returns only after the existing source loader succeeds.

`ensure_source_schema_ready(*, settings=None)` is a separate, Airflow-independent
schema-readiness boundary. It opens one configured PostgreSQL connection and
delegates to the existing `ensure_source_schema(connection)` database boundary;
the connection context commits on success and rolls back on failure. It returns
only `{schema, status}`. The DAG calls it once before source fan-out, so source
adapters do not race to run schema DDL independently.

## dbt Runtime Adapter Boundary

The Airflow-independent `finstream.orchestration` package also exposes the dbt
stage boundaries:

- `run_dbt_seed(*, project_dir=None, profiles_dir=None)` invokes `dbt seed --full-refresh`.
- `run_dbt_models(*, project_dir=None, profiles_dir=None)` invokes `dbt run`.
- `run_dbt_tests(*, project_dir=None, profiles_dir=None)` invokes `dbt test`.

FinStream's dbt seeds are small, authoritative version-controlled mappings, so
their full refresh intentionally reproduces seed schema and content changes
deterministically. It recreates only dbt seed relations; it does not reset
Bronze or source data, dbt model history, PostgreSQL volumes, or the database
as a whole. `dbt run` follows seed recreation and reconstructs dependent
analytical models.

They execute the existing dbt CLI through an argument list with no shell. By
default, both `--project-dir` and `--profiles-dir` resolve to the repository's
`dbt/` directory from the installed FinStream source location, rather than from
the process working directory. This default is intended for the editable
repository installation used for local development; callers may supply explicit
directories when needed. dbt profile environment variables remain dbt's
configuration contract and are inherited by the CLI process without being
interpreted by Airflow.

The V1 DAG first makes source-schema readiness the shared direct prerequisite of
every independent source task. All source tasks then fan in to `dbt_seed`, and
`dbt_seed` remains the sole direct prerequisite of `dbt_run`. The two quality
tasks are independent siblings downstream of `dbt_run`:

```text
source_schema
    -> Market / SEC / FRED source tasks
    -> dbt_seed
    -> dbt_run
        -> dbt_test
        -> quality_monitoring
```

Task results are only small success summaries and are not transformation inputs;
dependency edges, not source XCom payloads, control this sequence. `dbt_test` is
a blocking analytical-quality gate: a non-zero dbt result propagates as task
failure. `quality_monitoring` calls the read-only
`run_quality_monitoring(*, as_of, settings=None)` boundary with the deterministic
`dag_run.run_after.date()` value. It returns structured signals without storing
history, alerting, or thresholds. Returned monitoring `ERROR` signals remain
non-blocking observations; an unexpected runtime exception still fails that task.

## Task Communication and Results

Runtime adapters may use domain objects and database connections internally, but
task communication must not transfer implementation-specific or heavy objects
through XCom. This includes database connections, API clients, raw provider
payloads, PyArrow tables, `MarketBronzeResult`, `SecCompanyBronzeResult`, and
`FredSeriesBronzeResult`.

Public orchestration results are small JSON-serializable summaries composed only
of primitive values and simple collections. The current source result shapes are:

```text
Market: {source, symbol, run_id, record_count, records_loaded}
SEC: {source, cik, run_id, submissions_record_count,
      company_facts_record_count, submissions_loaded, company_facts_loaded}
FRED: {source, series_id, run_id, metadata_record_count,
       observations_record_count, metadata_loaded, observations_loaded}
dbt: {command, return_code}
quality monitoring: {as_of, signal_count, status_counts, signals}
```

## Retry, Failure, Idempotency, and `run_at`

Airflow retries are safe only because the underlying FinStream pipeline retains
its own deterministic Bronze recovery, PostgreSQL replay verification,
incremental-overlap, and idempotency behavior. Airflow must not duplicate,
replace, or compensate for those mechanisms, and runtime adapters do not add
retry loops solely for task retries.

| Task category | Retries | Delay | Failure semantics |
| --- | ---: | --- | --- |
| `source_schema` | 1 | 1 minute | Runtime failure retries. |
| Market, SEC, and FRED source tasks | 2 | 5 minutes | The complete entity task retries. |
| `dbt_seed` | 1 | 1 minute | Process or runtime failure retries. |
| `dbt_run` | 1 | 1 minute | Process or runtime failure retries. |
| `dbt_test` | 0 | none | Analytical failure is fail-fast. |
| `quality_monitoring` | 1 | 1 minute | Only an actual runtime exception retries. |

Returned monitoring `ERROR` signals remain non-blocking structured output and do
not trigger a retry. A monitoring runtime exception propagates normally. dbt
test failures also propagate normally, with no retry. Normal Airflow task
states and logs are the authoritative failure visibility; no callbacks, alert
destination, or custom status persistence is configured.

One source runtime invocation uses one shared timezone-aware `run_at` value for
all logically related Bronze artifacts. Its representation and run-identity
meaning remain defined by the existing Bronze contract. Airflow source tasks pass
their `dag_run.run_after` value unchanged as `run_at`. This value is deterministic
and timezone-aware for one DAG run. `ingested_at` remains that unmodified value;
the Bronze `run_id` additionally encodes the source boundary's normalized symbol,
CIK, or series ID. Therefore multiple entities may share one DAG-run timestamp
without sharing a Bronze or PostgreSQL run identity, while a retry or
clear-and-rerun of the same entity preserves its identity. Task wall-clock time,
`logical_date`, retry number, and synthetic per-entity timestamp offsets must not
be used. The monitoring task likewise reuses `dag_run.run_after.date()` on a
retry rather than a wall-clock date.

V1 is intentionally manually triggered: `schedule=None`, `catchup=False`, and
one DAG run owns one deterministic `run_after`. No automatic production cadence
is claimed; a future schedule requires separate requirements.

## Local Airflow Runtime and Dependency Boundary

Windows is the primary host environment. Apache Airflow is not treated as a
native Windows runtime; WSL2 is the supported non-containerized local runtime.
Airflow remains an optional dependency of FinStream core. Docker and Docker
Compose are separate infrastructure concerns.

`pyproject.toml` pins the optional group to `apache-airflow==3.3.2`. Install it
with Apache Airflow's matching official constraints file rather than an
unconstrained Airflow installation:

```bash
# Run inside a WSL2 Linux distribution, from the repository root.
export AIRFLOW_HOME="$PWD/.airflow"
python3.12 -m venv .venv-airflow-wsl
source .venv-airflow-wsl/bin/activate

export AIRFLOW_VERSION=3.3.2
export PYTHON_VERSION=3.12
export AIRFLOW_CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

python -m pip install --upgrade pip
python -m pip install -e '.[airflow]' --constraint "$AIRFLOW_CONSTRAINTS_URL"
airflow version
```

Core tests neither install nor import Airflow and must not start an Airflow
scheduler, API server, triggerer, DAG processor, or metadata database.

## DAG Design and Directory Rules

DAG definitions belong in [`../orchestration/dags`](../orchestration/dags), a
directory outside `src/` that has no `__init__.py` and is not named `airflow`.
Configure local discovery with:

```bash
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/orchestration/dags"
```

DAGs remain thin orchestration and wiring definitions that call FinStream
runtime boundaries. Business logic remains outside DAG definitions, and DAG
authors use Airflow 3's public API, for example `from airflow.sdk import DAG,
task`. No production scheduling cadence is defined.
