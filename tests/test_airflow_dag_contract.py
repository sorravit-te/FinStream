"""Static and optional-Airflow checks for the first FinStream pipeline DAG."""

import ast
import importlib.util
from pathlib import Path

import pytest

from finstream.fred.series import INITIAL_FRED_SERIES_IDS
from finstream.sec.companies import INITIAL_SEC_COMPANIES


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DAG_PATH = REPOSITORY_ROOT / "orchestration" / "dags" / "finstream_v1_pipeline.py"


def _expected_task_ids() -> set[str]:
    return {
        *(f"market_{company.ticker.lower()}" for company in INITIAL_SEC_COMPANIES),
        *(f"sec_{company.ticker.lower()}" for company in INITIAL_SEC_COMPANIES),
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
    assert source.count("dag_run.run_after") == 4
    assert "INITIAL_SEC_COMPANIES" in source
    assert "INITIAL_FRED_SERIES_IDS" in source
    assert "run_market_source(symbol, run_at=dag_run.run_after)" in source
    assert "run_sec_source(cik, run_at=dag_run.run_after)" in source
    assert "run_fred_source(series_id, run_at=dag_run.run_after)" in source
    assert "run_dbt_seed" in source
    assert "run_dbt_models" in source
    assert "run_dbt_tests" in source
    assert "run_quality_monitoring" in source
    assert "ensure_source_schema_ready" in source
    assert "@task(task_id=\"source_schema\")" in source
    assert "@task(task_id=\"dbt_seed\")" in source
    assert "@task(task_id=\"dbt_run\")" in source
    assert "@task(task_id=\"dbt_test\")" in source
    assert "@task(task_id=\"quality_monitoring\")" in source
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
        2 * len(INITIAL_SEC_COMPANIES) + len(INITIAL_FRED_SERIES_IDS)
    )
    assert len(expected_task_ids) == len(_source_task_ids()) + 5
    assert len(expected_task_ids) == 22
    assert "task_id=f\"market_{company.ticker.lower()}\"" in source
    assert "task_id=f\"sec_{company.ticker.lower()}\"" in source
    assert "task_id=f\"fred_{series_id.lower()}\"" in source


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
    for task_id in source_task_ids:
        task_instance = dag.get_task(task_id)
        assert task_instance.upstream_task_ids == {"source_schema"}
        assert task_instance.downstream_task_ids == {"dbt_seed"}

    dbt_seed = dag.get_task("dbt_seed")
    assert dbt_seed.upstream_task_ids == source_task_ids
    assert dbt_seed.downstream_task_ids == {"dbt_run"}

    dbt_run = dag.get_task("dbt_run")
    assert dbt_run.upstream_task_ids == {"dbt_seed"}
    assert dbt_run.downstream_task_ids == {"dbt_test", "quality_monitoring"}

    for task_id in {"dbt_test", "quality_monitoring"}:
        quality_task = dag.get_task(task_id)
        assert quality_task.upstream_task_ids == {"dbt_run"}
        assert not quality_task.downstream_task_ids
