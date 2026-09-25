"""Manually triggered source and dbt orchestration for FinStream V1."""

from datetime import timedelta

from airflow.sdk import dag, task
from airflow.sdk.types import DagRunProtocol

from finstream.fred.series import INITIAL_FRED_SERIES_IDS
from finstream.orchestration import (
    ensure_source_schema_ready,
    run_dbt_models,
    run_dbt_seed,
    run_dbt_tests,
    run_fred_source,
    run_market_source,
    run_sec_source,
    run_quality_monitoring,
)
from finstream.sec.companies import INITIAL_SEC_COMPANIES
from finstream.sec.companies import (
    HISTORICAL_SEC_REGISTRANTS,
    INITIAL_SEC_COMPANIES,
)


SOURCE_SCHEMA_RETRIES = 1
SOURCE_SCHEMA_RETRY_DELAY = timedelta(minutes=1)
SOURCE_RETRIES = 2
SOURCE_RETRY_DELAY = timedelta(minutes=5)
STANDARD_RETRIES = 1
STANDARD_RETRY_DELAY = timedelta(minutes=1)


@dag(
    dag_id="finstream_v1_pipeline",
    schedule=None,
    catchup=False,
)
def finstream_v1_pipeline():
    """Create source tasks followed by the FinStream dbt seed and run stages."""

    @task(retries=SOURCE_RETRIES, retry_delay=SOURCE_RETRY_DELAY)
    def market_source_task(
        symbol: str,
        dag_run: DagRunProtocol,
    ) -> dict[str, str | int]:
        return run_market_source(symbol, run_at=dag_run.run_after)

    @task(retries=SOURCE_RETRIES, retry_delay=SOURCE_RETRY_DELAY)
    def sec_source_task(
        cik: str,
        dag_run: DagRunProtocol,
    ) -> dict[str, str | int]:
        return run_sec_source(cik, run_at=dag_run.run_after)

    @task(retries=SOURCE_RETRIES, retry_delay=SOURCE_RETRY_DELAY)
    def fred_source_task(
        series_id: str,
        dag_run: DagRunProtocol,
    ) -> dict[str, str | int]:
        return run_fred_source(series_id, run_at=dag_run.run_after)

    @task(
        task_id="source_schema",
        retries=SOURCE_SCHEMA_RETRIES,
        retry_delay=SOURCE_SCHEMA_RETRY_DELAY,
    )
    def source_schema_task() -> dict[str, str]:
        return ensure_source_schema_ready()

    @task(
        task_id="dbt_seed",
        retries=STANDARD_RETRIES,
        retry_delay=STANDARD_RETRY_DELAY,
    )
    def dbt_seed_task() -> dict[str, str | int]:
        return run_dbt_seed()

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
    source_task_results = []
    for company in INITIAL_SEC_COMPANIES:
        source_task_results.append(
            market_source_task.override(task_id=f"market_{company.ticker.lower()}")(
                company.ticker
            )
        )
        source_task_results.append(
            sec_source_task.override(task_id=f"sec_{company.ticker.lower()}")(company.cik)
        )

    for registrant in HISTORICAL_SEC_REGISTRANTS:
        source_task_results.append(
            sec_source_task.override(task_id=registrant.task_key)(registrant.cik)
        )

    for series_id in INITIAL_FRED_SERIES_IDS:
        source_task_results.append(
            fred_source_task.override(task_id=f"fred_{series_id.lower()}")(series_id)
        )

    dbt_seed_result = dbt_seed_task()
    for source_task_result in source_task_results:
        source_schema_result >> source_task_result
        source_task_result >> dbt_seed_result
    dbt_run_result = dbt_run_task()
    dbt_seed_result >> dbt_run_result
    dbt_run_result >> dbt_test_task()
    dbt_run_result >> quality_monitoring_task()


finstream_v1_pipeline_dag = finstream_v1_pipeline()
