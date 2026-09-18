"""Cross-provider contracts for Step 7 PostgreSQL source loading."""

import ast
import inspect
import re
from pathlib import Path
from types import ModuleType

import finstream.database.connection as database_connection
import finstream.database.schema as database_schema
import finstream.fred.postgres as fred_postgres
import finstream.market.postgres as market_postgres
import finstream.sec.postgres as sec_postgres
from finstream.database.schema import SOURCE_SCHEMA_DDL


_LOADER_MODULES = (market_postgres, sec_postgres, fred_postgres)
_PROVIDER_PREFIXES = (
    "finstream.market",
    "finstream.sec",
    "finstream.fred",
)
_APPROVED_TABLES = {
    "source_data.ingestion_runs",
    "source_data.market_daily_prices",
    "source_data.sec_submissions",
    "source_data.sec_company_facts",
    "source_data.fred_series_metadata",
    "source_data.fred_series_observations",
}
_EXPECTED_PROVIDER_TABLES = _APPROVED_TABLES - {"source_data.ingestion_runs"}
_REGISTRY_COLUMNS = (
    "source",
    "dataset",
    "run_id",
    "ingested_at",
    "raw_json_path",
    "parquet_path",
    "record_count",
)
_LINEAGE_COLUMNS = ("source", "dataset", "run_id", "source_row_number")


def _module_source(module: ModuleType) -> str:
    return Path(module.__file__).read_text(encoding="utf-8")


def _imports(module: ModuleType) -> set[str]:
    tree = ast.parse(_module_source(module))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imported.add(node.module)
    return imported


def _insert_statements(module: ModuleType) -> tuple[str, ...]:
    return tuple(
        value
        for name, value in vars(module).items()
        if name.startswith("_INSERT_") and isinstance(value, str)
    )


def _insert_target(statement: str) -> str:
    match = re.search(r"\bINSERT\s+INTO\s+([a-z_]+\.[a-z_]+)", statement, re.I)
    assert match is not None
    return match.group(1).lower()


def _insert_columns(statement: str) -> tuple[str, ...]:
    match = re.search(
        r"\bINSERT\s+INTO\s+[a-z_]+\.[a-z_]+\s*\((.*?)\)\s*VALUES",
        statement,
        re.I | re.S,
    )
    assert match is not None
    return tuple(column.strip().lower() for column in match.group(1).split(","))


def test_provider_loaders_are_bronze_first_and_network_independent() -> None:
    forbidden_imports = {
        "requests",
        "finstream.market.twelve_data",
        "finstream.sec.edgar",
        "finstream.fred.client",
    }

    for module in _LOADER_MODULES:
        imports = _imports(module)
        assert imports.isdisjoint(forbidden_imports)
        assert "finstream.bronze.parquet_storage" in imports
        source = _module_source(module)
        assert "read_parquet" in source
        assert "read_raw_json" not in source
        assert "write_raw_json" not in source
        assert "write_parquet" not in source


def test_provider_loaders_do_not_import_one_another() -> None:
    own_prefix = {
        market_postgres: "finstream.market",
        sec_postgres: "finstream.sec",
        fred_postgres: "finstream.fred",
    }

    for module, allowed_provider_prefix in own_prefix.items():
        provider_imports = {
            name for name in _imports(module) if name.startswith(_PROVIDER_PREFIXES)
        }
        assert all(
            name.startswith(allowed_provider_prefix) for name in provider_imports
        )


def test_generic_database_package_remains_provider_independent() -> None:
    for module in (database_connection, database_schema):
        assert not any(
            name.startswith(_PROVIDER_PREFIXES) for name in _imports(module)
        )


def test_all_provider_sql_uses_shared_registry_and_lineage_contract() -> None:
    provider_targets: set[str] = set()
    for module in _LOADER_MODULES:
        statements = _insert_statements(module)
        registry_statements = [
            statement
            for statement in statements
            if _insert_target(statement) == "source_data.ingestion_runs"
        ]
        assert len(registry_statements) == 1
        assert _insert_columns(registry_statements[0]) == _REGISTRY_COLUMNS
        assert "loaded_at" not in registry_statements[0].lower()

        row_statements = [
            statement
            for statement in statements
            if _insert_target(statement) != "source_data.ingestion_runs"
        ]
        assert row_statements
        for statement in row_statements:
            assert _insert_columns(statement)[:4] == _LINEAGE_COLUMNS
            provider_targets.add(_insert_target(statement))

    assert provider_targets == _EXPECTED_PROVIDER_TABLES


def test_loader_sql_targets_only_step_7_source_tables() -> None:
    targets = {
        _insert_target(statement)
        for module in _LOADER_MODULES
        for statement in _insert_statements(module)
    }
    assert targets == _APPROVED_TABLES

    ddl_tables = {
        match.lower()
        for match in re.findall(
            r"\bCREATE TABLE IF NOT EXISTS\s+([a-z_]+\.[a-z_]+)",
            "\n".join(SOURCE_SCHEMA_DDL),
            re.I,
        )
    }
    assert ddl_tables == _APPROVED_TABLES


def test_loaders_leave_transactions_to_callers_and_open_no_connections() -> None:
    forbidden_methods = {"commit", "rollback", "close"}

    for module in _LOADER_MODULES:
        tree = ast.parse(_module_source(module))
        called_methods = {
            node.func.attr
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        }
        assigned_attributes = {
            target.attr
            for node in ast.walk(tree)
            if isinstance(node, (ast.Assign, ast.AnnAssign))
            for target in (
                node.targets if isinstance(node, ast.Assign) else [node.target]
            )
            if isinstance(target, ast.Attribute)
        }
        assert called_methods.isdisjoint(forbidden_methods)
        assert "connect" not in called_methods
        assert "autocommit" not in assigned_attributes


def test_step_7_loaders_have_no_rerun_or_transformation_policy() -> None:
    forbidden_sql = re.compile(r"\b(?:SELECT|ON\s+CONFLICT|MERGE|UPDATE|DELETE)\b", re.I)
    forbidden_tables = re.compile(r"\b(?:stg_|dim_|fact_|mart_)\w*", re.I)

    for module in _LOADER_MODULES:
        statements = _insert_statements(module)
        assert not any(forbidden_sql.search(statement) for statement in statements)
        assert not any(forbidden_tables.search(statement) for statement in statements)
        source = _module_source(module)
        assert "float(" not in source
        assert "sorted(" not in source
        assert ".sort(" not in source


def test_public_loaders_use_supplied_connection_cursor_contexts() -> None:
    expected_names = {
        market_postgres: {"load_market_bronze_to_postgres"},
        sec_postgres: {
            "load_sec_submissions_bronze_to_postgres",
            "load_sec_company_facts_bronze_to_postgres",
            "load_sec_company_bronze_to_postgres",
        },
        fred_postgres: {
            "load_fred_metadata_bronze_to_postgres",
            "load_fred_observations_bronze_to_postgres",
            "load_fred_series_bronze_to_postgres",
        },
    }

    for module, loader_names in expected_names.items():
        for loader_name in loader_names:
            source = inspect.getsource(getattr(module, loader_name))
            assert "connection.cursor()" in source
