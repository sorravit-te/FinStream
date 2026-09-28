# FinStream Runbook

## Prerequisites

- Windows PowerShell and the repository `.venv` with Python 3.12.
- Docker Desktop or another Docker Engine with Compose.
- Twelve Data and FRED API keys plus a valid SEC `User-Agent`.
- Standard On-premises Data Gateway for Power BI Service refresh only.

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
Copy-Item .env.docker.example .env.docker
```

Set the database and Airflow passwords, provider credentials,
`SEC_USER_AGENT`, `POSTGRES_DSN`, and `DBT_POSTGRES_SCHEMA` in `.env.docker`.
Keep credentials out of Git. Host tools connect to `localhost:5433`; containers
connect to `postgres:5432`.

## Start / Stop FinStream

Validate configuration, build the Airflow image, and start the platform:

```powershell
docker compose --env-file .env.docker config --quiet
docker compose --env-file .env.docker build airflow-init
docker compose --env-file .env.docker up -d
docker compose --env-file .env.docker ps
```

PostgreSQL and the Airflow API server, scheduler, and DAG processor should
become healthy. `airflow-init` is expected to migrate the metadata database and
exit successfully. Airflow is available at `http://localhost:8080`.

Stop containers, or remove them while preserving named volumes:

```powershell
docker compose --env-file .env.docker stop
docker compose --env-file .env.docker down
```

Restart with `docker compose --env-file .env.docker up -d`. Never use
`down -v` unless intentionally deleting PostgreSQL data, Bronze artifacts, and
Airflow state.

## Run Pipelines

Trigger the complete Market, SEC, and FRED workflow:

```powershell
docker compose --env-file .env.docker exec -T airflow-scheduler airflow dags unpause finstream_v1_pipeline
docker compose --env-file .env.docker exec -T airflow-scheduler airflow dags trigger finstream_v1_pipeline
docker compose --env-file .env.docker exec -T airflow-scheduler airflow tasks list finstream_v1_pipeline
```

`finstream_market_daily` runs Market-only ingestion Monday-Friday at 18:30
`America/New_York`, followed by dbt run, dbt tests, and quality monitoring. It
uses `catchup=False` and one active run. Restore a manually paused DAG with:

```powershell
docker compose --env-file .env.docker exec -T airflow-scheduler airflow dags unpause finstream_market_daily
```

The machine and Docker services must be available at the scheduled time. Missed
Airflow runs are not recreated; the three-calendar-day Market overlap recovers
missing or revised provider observations on the next execution. SEC and FRED
are not part of this daily DAG.

## Validate the Platform

Check containers, PostgreSQL, and DAG imports:

```powershell
docker compose --env-file .env.docker ps
docker compose --env-file .env.docker exec -T postgres sh -c 'pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
docker compose --env-file .env.docker exec -T airflow-scheduler airflow dags list
docker compose --env-file .env.docker exec -T airflow-scheduler airflow dags list-import-errors --output json
```

Expected results are healthy long-running services, PostgreSQL accepting
connections, both FinStream DAGs listed, and `[]` for import errors.

Validate dbt using the configured scheduler environment:

```powershell
docker compose --env-file .env.docker exec -T airflow-scheduler dbt parse --project-dir /opt/airflow/finstream/dbt --profiles-dir /opt/airflow/finstream/dbt
docker compose --env-file .env.docker exec -T airflow-scheduler dbt build --project-dir /opt/airflow/finstream/dbt --profiles-dir /opt/airflow/finstream/dbt
```

Run Python tests:

```powershell
.\.venv\Scripts\python.exe -m pytest -v
.\.venv\Scripts\python.exe -m pytest tests/orchestration -v
```

Live integration tests require a dedicated `FINSTREAM_TEST_POSTGRES_DSN`; do
not substitute the application database DSN.

## Power BI Refresh

Open `powerbi/FinStream.pbip` in Power BI Desktop. Refresh after PostgreSQL is
healthy and dbt validation succeeds; the checked-in source is
`PostgreSQL.Database("localhost:5433", "finstream")`.

Power BI Service refresh runs Tuesday-Saturday at 08:00 ICT (UTC+7). At that
time the computer must be awake, Docker and PostgreSQL must be available, and
the Standard Gateway service must be online. The browser, Power BI Desktop,
terminal, VS Code, and Docker Desktop window do not need to remain open.

## Recovery / Replay

- **Missed Market schedule:** Start the platform and unpause or manually trigger
  `finstream_market_daily`; incremental overlap recovers provider gaps without
  historical Airflow catchup.
- **FRED / SEC:** Trigger `finstream_v1_pipeline`. FRED observations use a
  365-day overlap with full metadata retrieval; SEC uses current/full retrieval.
- **Bronze:** Do not edit artifacts. Raw-only same-run data can reconstruct
  Parquet; Parquet-only, corrupt, mismatched, or unexpected files require
  investigation.
- **PostgreSQL:** Exact replay verification uses `(source, dataset, run_id)` and
  validates run metadata and row counts.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Host PostgreSQL connection fails | Use `localhost:5433`; `postgres:5432` is container-only. Check `POSTGRES_HOST_PORT` and container health. |
| Airflow UI is unavailable | Confirm `airflow-api-server` is healthy and port `8080` is free. |
| DAG is missing or broken | Run `airflow dags list-import-errors --output json` and inspect DAG processor logs. |
| Pipeline task fails immediately | Verify provider credentials, `SEC_USER_AGENT`, `POSTGRES_DSN`, and network access without printing secrets. |
| dbt reports missing environment variables | Run it in `airflow-scheduler`, or set every `DBT_POSTGRES_*` variable for host-side dbt. |
| Power BI Desktop cannot refresh | Confirm PostgreSQL at `localhost:5433` and all three `analytics` marts. |
| Power BI Service cannot refresh | Confirm the Standard Gateway service and PostgreSQL data-source mapping. |

Inspect service logs with `docker compose --env-file .env.docker logs <service-name>`
and use the Airflow UI for task-level failures.
