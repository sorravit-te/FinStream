# Runbook

## Install and Configure

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
Copy-Item .env.docker.example .env.docker
```

Fill `.env.docker` with database and Airflow passwords, provider credentials,
`SEC_USER_AGENT`, `POSTGRES_DSN`, and `DBT_POSTGRES_SCHEMA`. Keep it outside
Git. `pyproject.toml` is the dependency source of truth.

## Start Local Services

```powershell
docker compose --env-file .env.docker build airflow-init
docker compose --env-file .env.docker up -d
docker compose --env-file .env.docker ps
```

For hands-off local scheduling on Windows, enable Docker Desktop's option to
start when you sign in. The computer must be awake, Docker Engine must be
running, and the FinStream containers must be running when a schedule is due.
The Docker Desktop window, PowerShell, VS Code, browser, and Airflow UI do not
need to remain open. Long-running FinStream services use
`restart: unless-stopped`, so they recover after Docker Engine restarts unless
you intentionally stopped them.

Airflow is exposed at `http://localhost:8080` by default. Inspect services with:

```powershell
docker compose --env-file .env.docker logs airflow-api-server
docker compose --env-file .env.docker logs airflow-scheduler
docker compose --env-file .env.docker exec airflow-scheduler airflow dags list
docker compose --env-file .env.docker exec airflow-scheduler airflow dags list-import-errors
```

## Run the Pipelines

The literal DAG ID is `finstream_v1_pipeline`. It is manual (`schedule=None`,
`catchup=False`):

```powershell
docker compose --env-file .env.docker exec airflow-scheduler airflow dags unpause finstream_v1_pipeline
docker compose --env-file .env.docker exec airflow-scheduler airflow dags trigger finstream_v1_pipeline
```

Use Airflow task logs to diagnose failures. Provider calls require configured
credentials; starting Compose does not run the pipeline.

`finstream_market_daily` is the separate automated weekday Market batch DAG. It
runs at 18:30 America/New_York (Monday through Friday), retains `catchup=False`,
allows only one active run, and is unpaused by default when first created in a
fresh Airflow metadata database. Airflow derives the schedule in the named
timezone so US daylight-saving changes are handled correctly. Existing metadata
retains its current pause state; if the DAG was manually paused, unpause it with:

```powershell
docker compose --env-file .env.docker exec airflow-scheduler airflow dags unpause finstream_market_daily
```

At its scheduled time, Docker, Airflow, and PostgreSQL must be running locally.
The DAG ingests only the configured Market tickers, then runs dbt models, dbt
tests, and quality monitoring. It does not fetch SEC or FRED, refresh static dbt
seeds, stream real-time data, or trigger a Power BI refresh. Weekday scheduling
does not include an exchange-holiday calendar; established incremental Market
loading safely handles a run when no new trading date is available.

`catchup=False` prevents Airflow from creating one historical DAG run for every
weekday missed while the scheduler was offline. On the next schedulable
interval, the Market tasks independently recover source gaps: established
incremental ingestion starts from the latest stored trading date (with its
existing overlap) and requests through the provider's latest available date.
Historical Airflow catchup runs are therefore not enabled for data recovery.

## Validate Python and dbt

```powershell
.\.venv\Scripts\python.exe -m pytest
.\.venv\Scripts\dbt.exe deps --project-dir dbt --profiles-dir dbt
.\.venv\Scripts\dbt.exe parse --project-dir dbt --profiles-dir dbt
.\.venv\Scripts\dbt.exe build --project-dir dbt --profiles-dir dbt
```

dbt reads `DBT_POSTGRES_HOST`, `DBT_POSTGRES_PORT`, `DBT_POSTGRES_USER`,
`DBT_POSTGRES_PASSWORD`, `DBT_POSTGRES_DBNAME`, and `DBT_POSTGRES_SCHEMA`.
The dedicated `FINSTREAM_TEST_POSTGRES_DSN` is only for disposable integration
testing, not an application database replacement.

## Inspect Status and Data

```powershell
.\.venv\Scripts\python.exe -m finstream.operational_status --as-of 2026-09-27
.\.venv\Scripts\python.exe -m finstream.operational_status --test-dsn --as-of 2025-02-01 --json
docker compose --env-file .env.docker exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
```

The status command is read-only. It reports PostgreSQL reachability, provenance,
latest Bronze file-pair presence, expected marts, and informational recency. It
redacts credentials. Recency is not an SLA; an old date alone is not an error.

## Stop and Reset Safely

```powershell
docker compose --env-file .env.docker down
```

This stops services and preserves named volumes. Do not run `down -v` against a
developer environment unless intentionally discarding all local PostgreSQL,
Bronze, and Airflow state.

`docker compose stop` intentionally stops existing containers, and
`restart: unless-stopped` respects that stopped state. `docker compose down`
removes the containers, so their restart policies cannot recreate them; run
`docker compose up -d` again. `docker compose down -v` also destroys the named
persistent volumes and must not be used for routine shutdown.
