"""Manually triggered source-runtime orchestration for FinStream V1."""

from airflow.sdk import dag, task
from airflow.sdk.types import DagRunProtocol

from finstream.fred.series import INITIAL_FRED_SERIES_IDS
from finstream.orchestration import (
    run_fred_source,
    run_market_source,
    run_sec_source,
)
from finstream.sec.companies import INITIAL_SEC_COMPANIES


@dag(
    dag_id="finstream_v1_pipeline",
    schedule=None,
    catchup=False,
)
def finstream_v1_pipeline():
    """Create independent source tasks for the configured FinStream entities."""

    @task
    def market_source_task(
        symbol: str,
        dag_run: DagRunProtocol,
    ) -> dict[str, str | int]:
        return run_market_source(symbol, run_at=dag_run.run_after)

    @task
    def sec_source_task(
        cik: str,
        dag_run: DagRunProtocol,
    ) -> dict[str, str | int]:
        return run_sec_source(cik, run_at=dag_run.run_after)

    @task
    def fred_source_task(
        series_id: str,
        dag_run: DagRunProtocol,
    ) -> dict[str, str | int]:
        return run_fred_source(series_id, run_at=dag_run.run_after)

    for company in INITIAL_SEC_COMPANIES:
        market_source_task.override(task_id=f"market_{company.ticker.lower()}")(
            company.ticker
        )
        sec_source_task.override(task_id=f"sec_{company.ticker.lower()}")(
            company.cik
        )

    for series_id in INITIAL_FRED_SERIES_IDS:
        fred_source_task.override(task_id=f"fred_{series_id.lower()}")(series_id)


finstream_v1_pipeline_dag = finstream_v1_pipeline()
