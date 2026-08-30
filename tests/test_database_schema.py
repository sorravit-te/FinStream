import re
from types import ModuleType
from unittest.mock import MagicMock, Mock, call

import psycopg
import pytest

import finstream.database.schema as database_schema
from finstream.database.schema import (
    SOURCE_SCHEMA_DDL,
    SOURCE_SCHEMA_NAME,
    ensure_source_schema,
)


_TABLES = (
    "ingestion_runs",
    "market_daily_prices",
    "sec_submissions",
    "sec_company_facts",
    "fred_series_metadata",
    "fred_series_observations",
)
_DATA_TABLES = _TABLES[1:]


def _normalize(statement: str) -> str:
    return re.sub(r"\s+", " ", statement.strip()).lower()


def _table_ddl(table_name: str) -> str:
    prefix = f"create table if not exists {SOURCE_SCHEMA_NAME}.{table_name} "
    matches = [
        _normalize(statement)
        for statement in SOURCE_SCHEMA_DDL
        if _normalize(statement).startswith(prefix)
    ]
    assert len(matches) == 1
    return matches[0]


def _mock_connection() -> tuple[MagicMock, MagicMock, MagicMock]:
    connection = MagicMock(spec=psycopg.Connection)
    cursor_context = connection.cursor.return_value
    cursor = cursor_context.__enter__.return_value
    return connection, cursor_context, cursor


def test_schema_and_table_inventory_is_exact_and_dependency_ordered() -> None:
    combined_ddl = "\n".join(SOURCE_SCHEMA_DDL)
    declared_tables = re.findall(
        r"\bCREATE TABLE IF NOT EXISTS\s+([a-z_]+\.[a-z_]+)",
        combined_ddl,
        flags=re.IGNORECASE,
    )

    assert SOURCE_SCHEMA_NAME == "source_data"
    assert _normalize(SOURCE_SCHEMA_DDL[0]) == (
        "create schema if not exists source_data"
    )
    assert declared_tables == [
        f"source_data.{table_name}" for table_name in _TABLES
    ]
    assert len(SOURCE_SCHEMA_DDL) == 7
    assert not any(
        name.startswith(("stg_", "dim_", "fact_", "mart_"))
        for name in _TABLES
    )


def test_ddl_contains_no_destructive_or_data_manipulation_statements() -> None:
    combined_ddl = "\n".join(SOURCE_SCHEMA_DDL)

    assert not re.search(
        r"\b(?:DROP|TRUNCATE|INSERT|UPDATE|DELETE|MERGE|COPY)\b",
        combined_ddl,
        flags=re.IGNORECASE,
    )
    assert "CREATE VIEW" not in combined_ddl.upper()
    assert "CREATE MATERIALIZED VIEW" not in combined_ddl.upper()


def test_ingestion_runs_contract_preserves_bronze_event_identity() -> None:
    ddl = _table_ddl("ingestion_runs")
    ordered_columns = (
        "source text not null",
        "dataset text not null",
        "run_id text not null",
        "ingested_at timestamptz not null",
        "raw_json_path text not null",
        "parquet_path text not null",
        "record_count bigint not null",
        "loaded_at timestamptz not null default current_timestamp",
    )

    positions = [ddl.index(column) for column in ordered_columns]
    assert positions == sorted(positions)
    assert "constraint pk_ingestion_runs primary key (source, dataset, run_id)" in ddl
    assert "check (btrim(source) <> '')" in ddl
    assert "check (btrim(dataset) <> '')" in ddl
    assert "check (run_id ~ '^[0-9]{8}t[0-9]{12}z$')" in ddl
    assert "check (btrim(raw_json_path) <> '')" in ddl
    assert "check (btrim(parquet_path) <> '')" in ddl
    assert "check (raw_json_path <> parquet_path)" in ddl
    assert "check (record_count >= 0)" in ddl


@pytest.mark.parametrize("table_name", _DATA_TABLES)
def test_data_tables_share_lineage_primary_key_and_run_foreign_key(
    table_name: str,
) -> None:
    ddl = _table_ddl(table_name)
    lineage_columns = (
        "source text not null",
        "dataset text not null",
        "run_id text not null",
        "source_row_number bigint not null",
    )

    assert [ddl.index(column) for column in lineage_columns] == sorted(
        ddl.index(column) for column in lineage_columns
    )
    assert (
        "primary key (source, dataset, run_id, source_row_number)" in ddl
    )
    assert "foreign key (source, dataset, run_id)" in ddl
    assert (
        "references source_data.ingestion_runs (source, dataset, run_id)" in ddl
    )
    assert "check (source_row_number >= 0)" in ddl
    assert "on delete" not in ddl


@pytest.mark.parametrize(
    ("table_name", "source", "dataset"),
    [
        ("market_daily_prices", "twelve_data", "daily_market_prices"),
        ("sec_submissions", "sec_edgar", "submissions"),
        ("sec_company_facts", "sec_edgar", "company_facts"),
        ("fred_series_metadata", "fred", "series_metadata"),
        ("fred_series_observations", "fred", "series_observations"),
    ],
)
def test_each_data_table_locks_its_source_dataset_identity(
    table_name: str,
    source: str,
    dataset: str,
) -> None:
    ddl = _table_ddl(table_name)

    assert f"source = '{source}'" in ddl
    assert f"dataset = '{dataset}'" in ddl


def test_market_contract_preserves_precision_nullability_and_integrity() -> None:
    ddl = _table_ddl("market_daily_prices")

    for column in ("open", "high", "low", "close"):
        assert f"{column} numeric(38,18) not null" in ddl
    assert "symbol text not null" in ddl
    assert "trading_date date not null" in ddl
    assert "volume bigint null" in ddl
    assert "check (btrim(symbol) <> '')" in ddl
    assert "check (volume is null or volume >= 0)" in ddl
    for comparison in (
        "high >= low",
        "high >= open",
        "high >= close",
        "low <= open",
        "low <= close",
    ):
        assert f"check ({comparison})" in ddl
    assert (
        "unique (source, dataset, run_id, symbol, trading_date)" in ddl
    )


def test_sec_submissions_contract_preserves_source_fields_and_run_grain() -> None:
    ddl = _table_ddl("sec_submissions")

    expected_columns = (
        "cik text not null",
        "company_name text not null",
        "accession_number text not null",
        "filing_date date not null",
        "report_date date null",
        "acceptance_datetime timestamptz null",
        "form text not null",
        "act text null",
        "file_number text null",
        "film_number text null",
        "items text null",
        "size bigint not null",
        "is_xbrl boolean not null",
        "is_inline_xbrl boolean not null",
        "primary_document text null",
        "primary_doc_description text null",
    )

    assert all(column in ddl for column in expected_columns)
    assert "check (cik ~ '^[0-9]{10}$')" in ddl
    assert "check (btrim(company_name) <> '')" in ddl
    assert "check (btrim(accession_number) <> '')" in ddl
    assert "check (btrim(form) <> '')" in ddl
    assert "check (size >= 0)" in ddl
    assert "unique (source, dataset, run_id, accession_number)" in ddl


def test_sec_company_facts_contract_preserves_occurrence_rows() -> None:
    ddl = _table_ddl("sec_company_facts")
    expected_columns = (
        "cik text not null",
        "entity_name text not null",
        "taxonomy text not null",
        "concept text not null",
        "label text null",
        "description text null",
        "unit text not null",
        "value numeric(76,30) not null",
        "start_date date null",
        "end_date date not null",
        "accession_number text not null",
        "fiscal_year integer null",
        "fiscal_period text null",
        "form text not null",
        "filed_date date not null",
        "frame text null",
    )

    assert all(column in ddl for column in expected_columns)
    assert "check (cik ~ '^[0-9]{10}$')" in ddl
    for column in (
        "entity_name",
        "taxonomy",
        "concept",
        "unit",
        "accession_number",
        "form",
    ):
        assert f"check (btrim({column}) <> '')" in ddl
    assert "check (start_date is null or start_date <= end_date)" in ddl
    assert " unique (" not in ddl


def test_fred_metadata_contract_preserves_timestamps_ranges_and_run_grain() -> None:
    ddl = _table_ddl("fred_series_metadata")
    expected_columns = (
        "series_id text not null",
        "realtime_start date not null",
        "realtime_end date not null",
        "title text not null",
        "observation_start date not null",
        "observation_end date not null",
        "frequency text not null",
        "frequency_short text not null",
        "units text not null",
        "units_short text not null",
        "seasonal_adjustment text not null",
        "seasonal_adjustment_short text not null",
        "last_updated timestamptz not null",
        "popularity bigint not null",
        "notes text null",
    )

    assert all(column in ddl for column in expected_columns)
    for column in (
        "series_id",
        "title",
        "frequency",
        "frequency_short",
        "units",
        "units_short",
        "seasonal_adjustment",
        "seasonal_adjustment_short",
    ):
        assert f"btrim({column}) <> ''" in ddl
    assert "check (realtime_start <= realtime_end)" in ddl
    assert "check (observation_start <= observation_end)" in ddl
    assert "check (popularity >= 0)" in ddl
    assert "unique (source, dataset, run_id, series_id)" in ddl


def test_fred_observation_contract_preserves_missing_values_and_realtime_grain() -> None:
    ddl = _table_ddl("fred_series_observations")

    assert "series_id text not null" in ddl
    assert "realtime_start date not null" in ddl
    assert "realtime_end date not null" in ddl
    assert "observation_date date not null" in ddl
    assert "value numeric(76,30) null" in ddl
    assert "check (btrim(series_id) <> '')" in ddl
    assert "check (realtime_start <= realtime_end)" in ddl
    assert (
        "unique (source, dataset, run_id, series_id, observation_date, "
        "realtime_start, realtime_end)"
    ) in ddl


def test_exact_numeric_and_timezone_types_exclude_lossy_storage() -> None:
    combined_ddl = _normalize("\n".join(SOURCE_SCHEMA_DDL))

    assert combined_ddl.count("numeric(38,18)") == 4
    assert combined_ddl.count("numeric(76,30)") == 2
    assert combined_ddl.count("timestamptz") == 4
    assert not re.search(
        r"\b(?:real|float|money)\b|double\s+precision",
        combined_ddl,
    )


def test_ensure_source_schema_executes_once_in_caller_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection, cursor_context, cursor = _mock_connection()
    connect = Mock()
    monkeypatch.setattr(database_schema.psycopg, "connect", connect)

    ensure_source_schema(connection)

    connection.cursor.assert_called_once_with()
    cursor_context.__enter__.assert_called_once_with()
    cursor_context.__exit__.assert_called_once_with(None, None, None)
    assert cursor.execute.call_args_list == [
        call(statement) for statement in SOURCE_SCHEMA_DDL
    ]
    connection.commit.assert_not_called()
    connection.rollback.assert_not_called()
    connection.close.assert_not_called()
    connect.assert_not_called()


def test_ensure_source_schema_propagates_error_and_stops_execution() -> None:
    connection, cursor_context, cursor = _mock_connection()
    failure = psycopg.OperationalError("DDL failed")
    cursor.execute.side_effect = [None, None, failure]

    with pytest.raises(psycopg.OperationalError, match="DDL failed"):
        ensure_source_schema(connection)

    assert cursor.execute.call_args_list == [
        call(statement) for statement in SOURCE_SCHEMA_DDL[:3]
    ]
    assert cursor_context.__exit__.call_count == 1
    connection.commit.assert_not_called()
    connection.rollback.assert_not_called()
    connection.close.assert_not_called()


def test_database_schema_module_has_no_provider_package_dependencies() -> None:
    provider_prefixes = (
        "finstream.market",
        "finstream.sec",
        "finstream.fred",
    )

    for value in vars(database_schema).values():
        referenced_module = (
            value.__name__
            if isinstance(value, ModuleType)
            else getattr(value, "__module__", "")
        )
        assert not referenced_module.startswith(provider_prefixes)
