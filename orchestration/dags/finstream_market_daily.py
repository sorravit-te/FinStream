"""Weekday scheduled Market ingestion and analytics refresh for FinStream."""

from datetime import timedelta

import pendulum
from airflow.sdk import dag, task
from airflow.sdk.types import DagRunProtocol

from finstream.orchestration import (
    ensure_source_schema_ready,
    run_dbt_models,
    run_dbt_tests,
    run_market_source,
    run_quality_monitoring,
)
from finstream.sec.companies import INITIAL_SEC_COMPANIES


SOURCE_SCHEMA_RETRIES = 1
SOURCE_SCHEMA_RETRY_DELAY = timedelta(minutes=1)
SOURCE_RETRIES = 2
SOURCE_RETRY_DELAY = timedelta(minutes=5)
STANDARD_RETRIES = 1
STANDARD_RETRY_DELAY = timedelta(minutes=1)


@dag(
    dag_id="finstream_market_daily",
    schedule="30 18 * * 1-5",
    start_date=pendulum.datetime(2025, 1, 1, tz="America/New_York"),
    catchup=False,
    max_active_runs=1,
)
def finstream_market_daily():
    """Ingest configured Market symbols after each weekday US market close."""

    @task(retries=SOURCE_RETRIES, retry_delay=SOURCE_RETRY_DELAY)
    def market_source_task(
        symbol: str,
        dag_run: DagRunProtocol,
    ) -> dict[str, str | int]:
        return run_market_source(symbol, run_at=dag_run.run_after)

    @task(
        task_id="source_schema",
        retries=SOURCE_SCHEMA_RETRIES,
        retry_delay=SOURCE_SCHEMA_RETRY_DELAY,
    )
    def source_schema_task() -> dict[str, str]:
        return ensure_source_schema_ready()

    @task(
        task_id="dbt_run",
        retries=STANDARD_RETRIES,
        retry_delay=STANDARD_RETRY_DELAY,
    )
    def dbt_run_task() -> dict[str, str | int]:
        return run_dbt_models()

    @task(task_id="dbt_test", retries=0)
    def dbt_test_task() -> dict[str, str | int]:
        return run_dbt_tests()

    @task(
        task_id="quality_monitoring",
        retries=STANDARD_RETRIES,
        retry_delay=STANDARD_RETRY_DELAY,
    )
    def quality_monitoring_task(dag_run: DagRunProtocol) -> dict[str, object]:
        return run_quality_monitoring(as_of=dag_run.run_after.date())

    source_schema_result = source_schema_task()
    market_task_results = []
    for company in INITIAL_SEC_COMPANIES:
        market_task_results.append(
            market_source_task.override(task_id=f"market_{company.ticker.lower()}")(
                company.ticker
            )
        )

    dbt_run_result = dbt_run_task()
    for market_task_result in market_task_results:
        source_schema_result >> market_task_result
        market_task_result >> dbt_run_result
    dbt_test_result = dbt_test_task()
    quality_monitoring_result = quality_monitoring_task()
    dbt_run_result >> dbt_test_result >> quality_monitoring_result


finstream_market_daily_dag = finstream_market_daily()
