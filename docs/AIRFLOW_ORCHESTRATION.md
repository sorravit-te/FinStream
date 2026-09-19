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
```

## Retry, Idempotency, and `run_at`

Airflow retries are safe only because the underlying FinStream pipeline retains
its own deterministic Bronze recovery, PostgreSQL replay verification,
incremental-overlap, and idempotency behavior. Airflow must not duplicate,
replace, or compensate for those mechanisms, and runtime adapters do not add
retry loops solely for task retries.

One source runtime invocation uses one shared timezone-aware `run_at` value for
all logically related Bronze artifacts. Its representation and run-identity
meaning remain defined by the existing Bronze contract. Airflow source tasks pass
their `dag_run.run_after` value unchanged as `run_at`. This value is deterministic
and timezone-aware for one DAG run, so a retry or clear-and-rerun of a source task
preserves the same FinStream Bronze run identity. Task wall-clock time and
`logical_date` must not be used for this identity. No production scheduling
policy is defined here.

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
