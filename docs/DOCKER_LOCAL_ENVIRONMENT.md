# Docker Local Environment Contract

## Purpose and Scope

Docker Compose is the local-development and reproducibility environment for
FinStream. It is not a production deployment design. The implemented scope
includes PostgreSQL infrastructure and a reusable FinStream Airflow runtime
image. It provides an Airflow Compose runtime for metadata initialization,
local scheduling, DAG parsing, and local API/UI access. It does not add a
FinStream application service, dbt container, or runnable containerized
pipeline.

The existing non-containerized WSL2 Airflow workflow remains a valid,
independent development path. Docker Compose is a separate local runtime option
and must not change ingestion, Bronze, PostgreSQL loading, dbt, or data-quality
business semantics.

## Operating Environment

Windows is the primary host environment. Docker Desktop running Linux
containers, with WSL2-compatible workflows, is the intended local platform.
The Compose workflow uses Docker Compose V2 syntax (`docker compose`).

This local environment does not introduce Kubernetes, Kafka, Spark, reverse
proxies, cloud infrastructure, or other infrastructure outside current V1
requirements.

## Airflow Runtime Direction

The containerized local runtime uses Apache Airflow 3.3.2 with Python 3.12 and
`LocalExecutor`. Its service topology is:

| Service | Intended responsibility |
| --- | --- |
| `airflow-init` | One-shot Airflow metadata migration. |
| `airflow-api-server` | Airflow API and local UI endpoint. |
| `airflow-scheduler` | Task scheduling and execution coordination through `LocalExecutor`. |
| `airflow-dag-processor` | Standalone DAG parsing and processing required by Airflow 3. |

Airflow 3 requires the standalone DAG processor. The current FinStream workload
does not justify distributed execution, so this local environment does not add
`CeleryExecutor`, Redis, `airflow-worker`, or Flower.

The existing V1 DAG policy remains unchanged: `schedule=None`, `catchup=False`,
and manual triggering of `finstream_v1_pipeline`. Containerization does not
define an automatic or production schedule.

## PostgreSQL Boundary

`compose.yaml` provides one `postgres` service using
`postgres:16.15-bookworm`. It exposes container port `5432` through the
configurable `POSTGRES_HOST_PORT` host port, which defaults to `5433`, and
stores database state in the named `postgres_data` volume. The service health
check uses `pg_isready` against the bootstrap database context.

The FinStream analytical/source database and the Airflow metadata database are
separate logical responsibilities on this one local PostgreSQL service. The
initialization script creates separate non-superuser login roles and owned
databases from `.env.docker` values. Airflow metadata tables must never share
the FinStream analytical database. Bootstrap, FinStream, and Airflow role and
database identifiers must remain distinct.

The bootstrap/admin role is only for initialization and local administration.
Future FinStream runtime wiring uses the FinStream role. The Airflow runtime
uses the separate Airflow role and database only for its metadata. Application
schemas and tables remain owned by the existing application and
schema-initialization boundaries. The read-only
`docker/postgres/10-init-finstream-databases.sh` initializer creates only
databases and roles; `airflow-init` creates Airflow metadata tables through
`airflow db migrate`.

## FinStream Execution Boundary

Airflow tasks continue to call the existing Airflow-independent FinStream
runtime adapters. Docker files, Compose YAML, and DAG definitions must not take
ownership of business logic.

The local runtime preserves this dependency direction:

```text
source ingestion
    -> Bronze persistence
    -> PostgreSQL loading
    -> dbt seed
    -> dbt run
    -> dbt test / quality monitoring
```

Containerization must not change source validation, incremental processing, run
identity, idempotency or replay behavior, dbt model logic, or data-quality
policy.

## Airflow Image and Build Contract

`docker/airflow/Dockerfile` builds a reusable FinStream Airflow runtime image
from the official `apache/airflow:3.3.2-python3.12` base image. The repository
root is its build context. It runs as the base image's normal `airflow` user and
installs the editable FinStream project together with its declared dependencies
at image-build time, while explicitly retaining Apache Airflow `3.3.2` during
dependency resolution. Dependencies are not installed dynamically at container
startup.

The image copies `pyproject.toml`, `src/`, and `dbt/` to
`/opt/airflow/finstream`. The deterministic layout makes the editable
FinStream source resolve from `/opt/airflow/finstream/src` and preserves the
existing dbt runtime adapter default at `/opt/airflow/finstream/dbt`.
The image does not contain local credentials or runtime database configuration.

Build the image from the repository root:

```powershell
docker build -f docker/airflow/Dockerfile -t finstream-airflow:local .
```

Basic inspection commands are:

```powershell
docker run --rm --entrypoint python finstream-airflow:local --version
docker run --rm finstream-airflow:local version
docker run --rm --entrypoint dbt finstream-airflow:local --version
docker run --rm --entrypoint python finstream-airflow:local -m pip check
```

`compose.yaml` uses the shared `finstream-airflow:local` image for all Airflow
services and builds it once through `airflow-init` using the same repository-root
build context and Dockerfile. The runtime stays non-root as the image's
`airflow` user.

## Configuration and Secrets

The existing environment-driven configuration model remains authoritative.
Credentials, API keys, and database passwords must never be hard-coded in a
Dockerfile or Compose file, committed to Git, or baked into image layers.

The PostgreSQL Compose workflow uses a dedicated gitignored `.env.docker` file,
created from `.env.docker.example`. It is separate from the existing `.env`
host/WSL2 contract. In particular, container-to-container PostgreSQL access
uses the Compose service hostname, while host access and non-containerized WSL2
access remain explicitly distinguishable and do not silently rely on
`localhost`.

Existing host runtime configuration remains unchanged: Python uses
`POSTGRES_DSN`, while dbt uses `DBT_POSTGRES_HOST`, `DBT_POSTGRES_PORT`,
`DBT_POSTGRES_USER`, `DBT_POSTGRES_PASSWORD`, `DBT_POSTGRES_DBNAME`, and
`DBT_POSTGRES_SCHEMA`. The Airflow metadata connection is separate from those
application settings. A small image-local helper constructs it from the
Airflow PostgreSQL environment values with percent encoding for URI-reserved
characters; it does not log credentials. The helper uses the installed
`psycopg2` SQLAlchemy driver.

`AIRFLOW_API_JWT_SECRET` belongs only in the gitignored `.env.docker` file and
is supplied to the API server and scheduler through
`AIRFLOW__API_AUTH__JWT_SECRET`. The API server, scheduler, and task runtime use
the internal execution API URL
`http://airflow-api-server:8080/execution/`; container traffic never uses the
Windows host port.

Airflow uses its built-in Simple Auth Manager for this local-only runtime. On
the first API-server startup, its default local `admin` user receives a generated
password in the container's ephemeral generated-password file and API-server
logs. Retrieve it only when needed:

```powershell
docker compose --env-file .env.docker logs airflow-api-server | Select-String "Password for user"
```

The generated password file is not copied into the image, mounted from the host,
or tracked by Git. Recreating the API-server container can generate a new local
password. This development-only authentication behavior is not a production
authentication design.

## Persistent Runtime Data

The Compose implementation must make these persistence boundaries explicit:

- PostgreSQL data survives ordinary container recreation through a named or
  otherwise explicit persistent volume.
- Airflow service logs remain inspectable through Docker Compose logs.
- Ephemeral Python, cache, and build artifacts do not require persistence.

The PostgreSQL volume is named by Compose without a globally fixed external
name. `docker compose down` preserves it; only an explicit `down -v` removes
it. PostgreSQL files are not bind-mounted to a Windows directory.

## PostgreSQL Connection Contract

| Caller | Host | Port |
| --- | --- | --- |
| Windows or WSL2 host process | `localhost` | `POSTGRES_HOST_PORT` (example: `5433`) |
| Airflow Compose service | `postgres` | `5432` |

Compose's normal project isolation and service DNS provide the container
hostname. No custom network or fixed container name is used.

## Networking and Exposed Interfaces

The API/UI service exposes container port `8080` only through
`127.0.0.1:${AIRFLOW_HOST_PORT:-8080}`. It is available locally at
`http://localhost:<AIRFLOW_HOST_PORT>`. PostgreSQL remains exposed for existing
local development, dbt, and BI workflows. Container-to-container traffic uses
Compose DNS service names. External source APIs remain unwired in this runtime.

No reverse proxy or additional network infrastructure is part of this contract.
PostgreSQL host exposure is configurable through `POSTGRES_HOST_PORT`, with an
example/default of `5433`. Airflow API/UI host exposure is configurable through
`AIRFLOW_HOST_PORT`, with an example/default of `8080`. Container-to-container
traffic continues to use Compose service DNS and internal container ports.

## Local Runtime Commands

Copy the tracked template before any Compose command, then fill in every
required blank password and secret value in `.env.docker`:

```powershell
Copy-Item .env.docker.example .env.docker
```

Build the shared custom image and start the local runtime. The one-shot init
service waits for PostgreSQL health and runs `airflow db migrate`; the API
server, scheduler, and DAG processor wait for that successful completion.

```powershell
docker compose --env-file .env.docker build airflow-init
docker compose --env-file .env.docker up -d
```

Inspect service health and logs:

```powershell
docker compose --env-file .env.docker ps
docker compose --env-file .env.docker logs airflow-api-server
docker compose --env-file .env.docker logs airflow-scheduler
docker compose --env-file .env.docker logs airflow-dag-processor
```

Inspect the API health, discovered DAGs, and import errors without triggering a
DAG run. `8080` is the default/example `AIRFLOW_HOST_PORT`; if it is changed,
use the configured host port in the API health URL:

```powershell
Invoke-WebRequest http://localhost:8080/api/v2/monitor/health
docker compose --env-file .env.docker exec airflow-scheduler airflow dags list
docker compose --env-file .env.docker exec airflow-scheduler airflow dags list-import-errors
```

Stop services without deleting persistent database state:

```powershell
docker compose --env-file .env.docker down
```

Reset the PostgreSQL environment only when deleting all local Compose database
data is intentional:

```powershell
docker compose --env-file .env.docker down -v
```

`down -v` is destructive. PostgreSQL's official initialization scripts run only
when the data directory is empty. Changing bootstrap database, user, or password
values does not rewrite an existing initialized volume; use an intentional
migration or an explicit destructive local reset.

The local runtime is reproducible from repository documentation plus local,
uncommitted credentials and configuration.

## Non-Goals

- Production deployment or automatic Airflow scheduling.
- Celery, Redis, distributed execution, or Kubernetes.
- Cloud infrastructure.
- `DockerOperator` task isolation.
- New source or transformation behavior.
- New analytics marts or Power BI implementation.
- CI/CD.

## Future Validation Expectations

The local runtime validates:

- Docker Compose configuration, custom image build, and non-root service users.
- PostgreSQL health, separate database and role creation, Airflow metadata
  migration, metadata-database connectivity, and database isolation.
- API, scheduler, and DAG-processor component health.
- Loopback API/UI reachability, LocalExecutor configuration, the internal
  execution API hostname, read-only DAG distribution, and DAG discovery.

The following remain outside the implemented local environment: provider and
FinStream runtime credentials, Bronze volume wiring and persistence, persistent
Airflow logs or Simple Auth credentials, containerized dbt execution, a
FinStream DAG run, and complete pipeline validation.
