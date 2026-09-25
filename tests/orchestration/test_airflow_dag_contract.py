"""Static and optional-Airflow checks for the first FinStream pipeline DAG."""

import ast
from datetime import timedelta
import importlib.util
from pathlib import Path

import pytest

from finstream.fred.series import INITIAL_FRED_SERIES_IDS
from finstream.sec.companies import INITIAL_SEC_COMPANIES
from finstream.sec.companies import (
    HISTORICAL_SEC_REGISTRANTS,
    INITIAL_SEC_COMPANIES,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
DAG_PATH = REPOSITORY_ROOT / "orchestration" / "dags" / "finstream_v1_pipeline.py"


def _module_assignments(tree: ast.Module) -> dict[str, ast.expr]:
    assignments: dict[str, ast.expr] = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name):
                assignments[target.id] = node.value
    return assignments


def _task_decorator_keywords(tree: ast.Module) -> dict[str, dict[str, ast.expr]]:
    task_keywords: dict[str, dict[str, ast.expr]] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        for decorator in node.decorator_list:
            if not (
                isinstance(decorator, ast.Call)
                and isinstance(decorator.func, ast.Name)
                and decorator.func.id == "task"
            ):
                continue
            task_keywords[node.name] = {
                keyword.arg: keyword.value
                for keyword in decorator.keywords
                if keyword.arg is not None
            }
    return task_keywords


def _assert_timedelta_minutes(value: ast.expr, minutes: int) -> None:
    assert isinstance(value, ast.Call)
    assert isinstance(value.func, ast.Name)
    assert value.func.id == "timedelta"
    assert len(value.keywords) == 1
    keyword = value.keywords[0]
    assert keyword.arg == "minutes"
    assert isinstance(keyword.value, ast.Constant)
    assert keyword.value.value == minutes


def _expected_task_ids() -> set[str]:
    return {
        *(f"market_{company.ticker.lower()}" for company in INITIAL_SEC_COMPANIES),
        *(f"sec_{company.ticker.lower()}" for company in INITIAL_SEC_COMPANIES),
        *(registrant.task_key for registrant in HISTORICAL_SEC_REGISTRANTS),
        *(f"fred_{series_id.lower()}" for series_id in INITIAL_FRED_SERIES_IDS),
        "source_schema",
        "dbt_seed",
        "dbt_run",
        "dbt_test",
        "quality_monitoring",
    }


def _source_task_ids() -> set[str]:
    return _expected_task_ids() - {
        "source_schema",
        "dbt_seed",
        "dbt_run",
        "dbt_test",
        "quality_monitoring",
    }


def test_dag_source_uses_the_approved_static_contract() -> None:
    source = DAG_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)

    assert "from airflow.sdk import dag, task" in source
    assert "from airflow.sdk.types import DagRunProtocol" in source
    assert "airflow.models" not in source
    assert "airflow.decorators" not in source
    assert "airflow.operators.python" not in source
    assert "datetime.now" not in source
    assert "datetime.utcnow" not in source
    assert "pendulum.now" not in source
    assert "logical_date" not in source
    assert "try_number" not in source
    assert "retry_exponential_backoff" not in source
    assert "max_retry_delay" not in source
    assert source.count("dag_run.run_after") == 4
    assert "INITIAL_SEC_COMPANIES" in source
    assert "HISTORICAL_SEC_REGISTRANTS" in source
    assert "INITIAL_FRED_SERIES_IDS" in source
    assert "run_market_source(symbol, run_at=dag_run.run_after)" in source
    assert "run_sec_source(cik, run_at=dag_run.run_after)" in source
    assert "run_fred_source(series_id, run_at=dag_run.run_after)" in source
    assert "run_dbt_seed" in source
    assert "run_dbt_models" in source
    assert "run_dbt_tests" in source
    assert "run_quality_monitoring" in source
    assert "ensure_source_schema_ready" in source
    for task_id in (
        "source_schema",
        "dbt_seed",
        "dbt_run",
        "dbt_test",
        "quality_monitoring",
    ):
        assert f'task_id="{task_id}"' in source
    assert "source_schema_result >> source_task_result" in source
    assert "source_task_result >> dbt_seed_result" in source
    assert "dbt_seed_result >> dbt_run_result" in source
    assert "dbt_run_result >> dbt_test_task()" in source
    assert "dbt_run_result >> quality_monitoring_task()" in source
    assert "run_quality_monitoring(as_of=dag_run.run_after.date())" in source
    assert "dbt build" not in source.lower()
    assert "date.today" not in source.lower()

    dag_decorator = next(
        decorator
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "finstream_v1_pipeline"
        for decorator in node.decorator_list
        if isinstance(decorator, ast.Call)
        and isinstance(decorator.func, ast.Name)
        and decorator.func.id == "dag"
    )
    keywords = {keyword.arg: keyword.value for keyword in dag_decorator.keywords}
    assert isinstance(keywords["dag_id"], ast.Constant)
    assert keywords["dag_id"].value == "finstream_v1_pipeline"
    assert isinstance(keywords["schedule"], ast.Constant)
    assert keywords["schedule"].value is None
    assert isinstance(keywords["catchup"], ast.Constant)
    assert keywords["catchup"].value is False

    assignments = _module_assignments(tree)
    assert isinstance(assignments["SOURCE_SCHEMA_RETRIES"], ast.Constant)
    assert assignments["SOURCE_SCHEMA_RETRIES"].value == 1
    _assert_timedelta_minutes(assignments["SOURCE_SCHEMA_RETRY_DELAY"], 1)
    assert isinstance(assignments["SOURCE_RETRIES"], ast.Constant)
    assert assignments["SOURCE_RETRIES"].value == 2
    _assert_timedelta_minutes(assignments["SOURCE_RETRY_DELAY"], 5)
    assert isinstance(assignments["STANDARD_RETRIES"], ast.Constant)
    assert assignments["STANDARD_RETRIES"].value == 1
    _assert_timedelta_minutes(assignments["STANDARD_RETRY_DELAY"], 1)

    task_keywords = _task_decorator_keywords(tree)
    for task_name in ("market_source_task", "sec_source_task", "fred_source_task"):
        assert isinstance(task_keywords[task_name]["retries"], ast.Name)
        assert task_keywords[task_name]["retries"].id == "SOURCE_RETRIES"
        assert isinstance(task_keywords[task_name]["retry_delay"], ast.Name)
        assert task_keywords[task_name]["retry_delay"].id == "SOURCE_RETRY_DELAY"
    expected_standard_tasks = {
        "source_schema_task": ("SOURCE_SCHEMA_RETRIES", "SOURCE_SCHEMA_RETRY_DELAY"),
        "dbt_seed_task": ("STANDARD_RETRIES", "STANDARD_RETRY_DELAY"),
        "dbt_run_task": ("STANDARD_RETRIES", "STANDARD_RETRY_DELAY"),
        "quality_monitoring_task": ("STANDARD_RETRIES", "STANDARD_RETRY_DELAY"),
    }
    for task_name, (retries_name, delay_name) in expected_standard_tasks.items():
        assert isinstance(task_keywords[task_name]["retries"], ast.Name)
        assert task_keywords[task_name]["retries"].id == retries_name
        assert isinstance(task_keywords[task_name]["retry_delay"], ast.Name)
        assert task_keywords[task_name]["retry_delay"].id == delay_name
    assert isinstance(task_keywords["dbt_test_task"]["retries"], ast.Constant)
    assert task_keywords["dbt_test_task"]["retries"].value == 0
    assert "retry_delay" not in task_keywords["dbt_test_task"]

    task_functions = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        for decorator in node.decorator_list:
            if isinstance(decorator, ast.Name) and decorator.id == "task":
                task_functions.add(node.name)
            if (
                isinstance(decorator, ast.Call)
                and isinstance(decorator.func, ast.Name)
                and decorator.func.id == "task"
            ):
                task_functions.add(node.name)
    assert task_functions == {
        "market_source_task",
        "sec_source_task",
        "fred_source_task",
        "source_schema_task",
        "dbt_seed_task",
        "dbt_run_task",
        "dbt_test_task",
        "quality_monitoring_task",
    }


def test_static_task_id_convention_covers_each_configured_entity() -> None:
    source = DAG_PATH.read_text(encoding="utf-8")
    expected_task_ids = _expected_task_ids()

    assert len(_source_task_ids()) == (
        2 * len(INITIAL_SEC_COMPANIES)
        + len(HISTORICAL_SEC_REGISTRANTS)
        + len(INITIAL_FRED_SERIES_IDS)
    )
    assert len(expected_task_ids) == len(_source_task_ids()) + 5
    assert len(expected_task_ids) == 23
    assert "task_id=f\"market_{company.ticker.lower()}\"" in source
    assert "task_id=f\"sec_{company.ticker.lower()}\"" in source
    assert "task_id=registrant.task_key" in source
    assert "task_id=f\"fred_{series_id.lower()}\"" in source


def test_orchestration_runtimes_do_not_define_airflow_retry_policy() -> None:
    runtime_directory = REPOSITORY_ROOT / "src" / "finstream" / "orchestration"

    for runtime_path in runtime_directory.glob("*.py"):
        source = runtime_path.read_text(encoding="utf-8").lower()
        assert "retry_delay" not in source
        assert "retry_exponential_backoff" not in source
        assert "max_retry_delay" not in source


@pytest.mark.airflow
def test_dag_import_and_task_structure_when_airflow_is_installed() -> None:
    pytest.importorskip("airflow")
    spec = importlib.util.spec_from_file_location("finstream_v1_pipeline", DAG_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    dag = module.finstream_v1_pipeline_dag
    expected_task_ids = _expected_task_ids()
    source_task_ids = _source_task_ids()

    assert dag.dag_id == "finstream_v1_pipeline"
    assert dag.schedule is None
    assert dag.catchup is False
    assert set(dag.task_ids) == expected_task_ids
    assert len(dag.task_ids) == len(expected_task_ids)
    source_schema = dag.get_task("source_schema")
    assert not source_schema.upstream_task_ids
    assert source_schema.downstream_task_ids == source_task_ids
    assert source_schema.retries == 1
    assert source_schema.retry_delay == timedelta(minutes=1)
    for task_id in source_task_ids:
        task_instance = dag.get_task(task_id)
        assert task_instance.upstream_task_ids == {"source_schema"}
        assert task_instance.downstream_task_ids == {"dbt_seed"}
        assert task_instance.retries == 2
        assert task_instance.retry_delay == timedelta(minutes=5)

    dbt_seed = dag.get_task("dbt_seed")
    assert dbt_seed.upstream_task_ids == source_task_ids
    assert dbt_seed.downstream_task_ids == {"dbt_run"}
    assert dbt_seed.retries == 1
    assert dbt_seed.retry_delay == timedelta(minutes=1)

    dbt_run = dag.get_task("dbt_run")
    assert dbt_run.upstream_task_ids == {"dbt_seed"}
    assert dbt_run.downstream_task_ids == {"dbt_test", "quality_monitoring"}
    assert dbt_run.retries == 1
    assert dbt_run.retry_delay == timedelta(minutes=1)

    dbt_test = dag.get_task("dbt_test")
    assert dbt_test.upstream_task_ids == {"dbt_run"}
    assert not dbt_test.downstream_task_ids
    assert dbt_test.retries == 0

    quality_monitoring = dag.get_task("quality_monitoring")
    assert quality_monitoring.upstream_task_ids == {"dbt_run"}
    assert not quality_monitoring.downstream_task_ids
    assert quality_monitoring.retries == 1
    assert quality_monitoring.retry_delay == timedelta(minutes=1)
