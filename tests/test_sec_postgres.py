from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock, call

import psycopg
import pyarrow as pa
import pytest

import finstream.sec.postgres as sec_postgres
from finstream.bronze.json_storage import write_raw_json
from finstream.bronze.models import BronzeRunLocation
from finstream.bronze.parquet_storage import BronzeParquetReadError, write_parquet
from finstream.sec.bronze import (
    SEC_BRONZE_SOURCE,
    SEC_COMPANY_FACTS_BRONZE_DATASET,
    SEC_COMPANY_FACTS_SCHEMA,
    SEC_SUBMISSIONS_BRONZE_DATASET,
    SEC_SUBMISSIONS_SCHEMA,
    SecBronzeDatasetResult,
    SecCompanyBronzeResult,
)
from finstream.sec.postgres import (
    SecPostgresLoadError,
    SecPostgresLoadResult,
    load_sec_company_bronze_to_postgres,
    load_sec_company_facts_bronze_to_postgres,
    load_sec_submissions_bronze_to_postgres,
)


_CIK = "0000320193"
_RUN_AT = datetime(2026, 9, 18, 4, 5, 6, 123456, tzinfo=timezone.utc)


def _submission_rows() -> list[dict[str, object]]:
    return [
        {
            "cik": _CIK,
            "company_name": "Apple Inc.",
            "accession_number": "0000001234-26-123456",
            "filing_date": date(2026, 8, 1),
            "report_date": date(2026, 6, 30),
            "acceptance_datetime": datetime(
                2026, 8, 1, 12, 1, 2, tzinfo=timezone(timedelta(hours=2))
            ),
            "form": "10-Q",
            "act": "34",
            "file_number": "001-36743",
            "film_number": "261234567",
            "items": None,
            "size": 123456,
            "is_xbrl": True,
            "is_inline_xbrl": True,
            "primary_document": "form10-q.htm",
            "primary_doc_description": None,
        },
        {
            "cik": _CIK,
            "company_name": "Apple Inc.",
            "accession_number": "0000001234-26-654321",
            "filing_date": date(2026, 7, 1),
            "report_date": None,
            "acceptance_datetime": None,
            "form": "8-K",
            "act": None,
            "file_number": None,
            "film_number": None,
            "items": None,
            "size": 42,
            "is_xbrl": False,
            "is_inline_xbrl": False,
            "primary_document": None,
            "primary_doc_description": None,
        },
    ]


def _company_fact_rows() -> list[dict[str, object]]:
    return [
        {
            "cik": _CIK,
            "entity_name": "Apple Inc.",
            "taxonomy": "us-gaap",
            "concept": "RevenueFromContractWithCustomerExcludingAssessedTax",
            "label": "Revenue",
            "description": "Revenue from contracts",
            "unit": "USD",
            "value": Decimal("123456.123456789012345678901234567890"),
            "start_date": date(2025, 1, 1),
            "end_date": date(2025, 3, 31),
            "accession_number": "0000001234-25-000057",
            "fiscal_year": 2025,
            "fiscal_period": "Q2",
            "form": "10-Q",
            "filed_date": date(2025, 5, 2),
            "frame": "CY2025Q1",
        },
        {
            "cik": _CIK,
            "entity_name": "Apple Inc.",
            "taxonomy": "us-gaap",
            "concept": "Assets",
            "label": None,
            "description": None,
            "unit": "USD",
            "value": Decimal("-25.000000000000000000000000000000"),
            "start_date": None,
            "end_date": date(2025, 6, 30),
            "accession_number": "0000001234-25-000058",
            "fiscal_year": None,
            "fiscal_period": None,
            "form": "10-Q",
            "filed_date": date(2025, 8, 1),
            "frame": None,
        },
    ]


def _table(dataset: str, rows: list[dict[str, object]] | None = None) -> pa.Table:
    schema = (
        SEC_SUBMISSIONS_SCHEMA
        if dataset == SEC_SUBMISSIONS_BRONZE_DATASET
        else SEC_COMPANY_FACTS_SCHEMA
    )
    return pa.Table.from_pylist([] if rows is None else rows, schema=schema)


def _result(
    tmp_path: Path,
    *,
    dataset: str,
    table: pa.Table | None = None,
    source: str = SEC_BRONZE_SOURCE,
    cik: str = _CIK,
    run_at: datetime = _RUN_AT,
) -> SecBronzeDatasetResult:
    location = BronzeRunLocation.from_run(
        root=tmp_path / "bronze",
        source=source,
        dataset=dataset,
        ingested_at=run_at,
    )
    write_raw_json(location, {"source": source, "dataset": dataset})
    persisted_table = _table(dataset) if table is None else table
    return SecBronzeDatasetResult(
        cik=cik,
        location=location,
        raw_json_path=location.directory / "payload.json",
        parquet_path=write_parquet(location, persisted_table),
        record_count=persisted_table.num_rows,
    )


def _mock_connection() -> tuple[MagicMock, MagicMock, MagicMock]:
    connection = MagicMock(spec=psycopg.Connection)
    cursor_context = connection.cursor.return_value
    cursor = cursor_context.__enter__.return_value
    cursor.fetchone.return_value = (1,)
    return connection, cursor_context, cursor


def _configure_combined_first_load(cursor: MagicMock) -> None:
    cursor.fetchone.side_effect = [None, None, (1,), (1,)]


def _registry_row(result: SecBronzeDatasetResult) -> tuple[object, ...]:
    metadata = result.location.metadata
    return (
        metadata.source,
        metadata.dataset,
        metadata.run_id,
        metadata.ingested_at,
        str(result.raw_json_path),
        str(result.parquet_path),
        result.record_count,
    )


def _configure_verified_replay(
    cursor: MagicMock,
    result: SecBronzeDatasetResult,
) -> None:
    cursor.fetchone.side_effect = [
        None,
        _registry_row(result),
        (result.record_count,),
    ]


def _assert_caller_owns_transaction(connection: MagicMock) -> None:
    connection.commit.assert_not_called()
    connection.rollback.assert_not_called()
    connection.close.assert_not_called()


def test_loads_submissions_in_parquet_order_with_timezone_and_nulls(
    tmp_path: Path,
) -> None:
    result = _result(
        tmp_path,
        dataset=SEC_SUBMISSIONS_BRONZE_DATASET,
        table=_table(SEC_SUBMISSIONS_BRONZE_DATASET, _submission_rows()),
    )
    connection, cursor_context, cursor = _mock_connection()

    assert load_sec_submissions_bronze_to_postgres(connection, result) == 2

    connection.cursor.assert_called_once_with()
    cursor_context.__enter__.assert_called_once_with()
    registry_sql, registry_values = cursor.execute.call_args.args
    assert "INSERT INTO source_data.ingestion_runs" in registry_sql
    assert "loaded_at" not in registry_sql
    assert registry_values[-1] == 2
    rows_sql, rows = cursor.executemany.call_args.args
    rows = list(rows)
    assert "INSERT INTO source_data.sec_submissions" in rows_sql
    assert "%s" in rows_sql
    assert [row[3] for row in rows] == [0, 1]
    assert [row[6] for row in rows] == [
        "0000001234-26-123456",
        "0000001234-26-654321",
    ]
    assert rows[0][9] == datetime(2026, 8, 1, 10, 1, 2, tzinfo=timezone.utc)
    assert rows[1][8:10] == (None, None)
    assert rows[0][14] is None
    _assert_caller_owns_transaction(connection)


def test_exact_sec_submissions_replay_is_a_verified_no_op(tmp_path: Path) -> None:
    result = _result(
        tmp_path,
        dataset=SEC_SUBMISSIONS_BRONZE_DATASET,
        table=_table(SEC_SUBMISSIONS_BRONZE_DATASET, _submission_rows()),
    )
    connection, _, cursor = _mock_connection()
    _configure_verified_replay(cursor, result)

    assert load_sec_submissions_bronze_to_postgres(connection, result) == 2

    assert cursor.execute.call_count == 3
    cursor.executemany.assert_not_called()
    _assert_caller_owns_transaction(connection)


def test_loads_zero_row_submissions_registry_only(tmp_path: Path) -> None:
    result = _result(tmp_path, dataset=SEC_SUBMISSIONS_BRONZE_DATASET)
    connection, _, cursor = _mock_connection()

    assert load_sec_submissions_bronze_to_postgres(connection, result) == 0

    assert cursor.execute.call_args.args[1][-1] == 0
    cursor.executemany.assert_not_called()
    _assert_caller_owns_transaction(connection)


def test_loads_company_facts_with_decimal_order_and_nulls(tmp_path: Path) -> None:
    result = _result(
        tmp_path,
        dataset=SEC_COMPANY_FACTS_BRONZE_DATASET,
        table=_table(SEC_COMPANY_FACTS_BRONZE_DATASET, _company_fact_rows()),
    )
    connection, _, cursor = _mock_connection()

    assert load_sec_company_facts_bronze_to_postgres(connection, result) == 2

    rows_sql, rows = cursor.executemany.call_args.args
    rows = list(rows)
    assert "INSERT INTO source_data.sec_company_facts" in rows_sql
    assert [row[3] for row in rows] == [0, 1]
    assert [row[7] for row in rows] == [
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "Assets",
    ]
    assert isinstance(rows[0][11], Decimal)
    assert rows[1][8:10] == (None, None)
    assert rows[1][12] is None
    assert rows[1][15:17] == (None, None)
    assert rows[1][19] is None
    _assert_caller_owns_transaction(connection)


def test_exact_sec_company_facts_replay_is_a_verified_no_op(tmp_path: Path) -> None:
    result = _result(
        tmp_path,
        dataset=SEC_COMPANY_FACTS_BRONZE_DATASET,
        table=_table(SEC_COMPANY_FACTS_BRONZE_DATASET, _company_fact_rows()),
    )
    connection, _, cursor = _mock_connection()
    _configure_verified_replay(cursor, result)

    assert load_sec_company_facts_bronze_to_postgres(connection, result) == 2

    assert cursor.execute.call_count == 3
    cursor.executemany.assert_not_called()


def test_loads_zero_row_company_facts_registry_only(tmp_path: Path) -> None:
    result = _result(tmp_path, dataset=SEC_COMPANY_FACTS_BRONZE_DATASET)
    connection, _, cursor = _mock_connection()

    assert load_sec_company_facts_bronze_to_postgres(connection, result) == 0

    assert cursor.execute.call_args.args[1][-1] == 0
    cursor.executemany.assert_not_called()


def test_combined_load_preflights_both_then_uses_one_cursor_in_dataset_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    submissions = _result(
        tmp_path,
        dataset=SEC_SUBMISSIONS_BRONZE_DATASET,
        table=_table(SEC_SUBMISSIONS_BRONZE_DATASET, _submission_rows()),
    )
    facts = _result(
        tmp_path,
        dataset=SEC_COMPANY_FACTS_BRONZE_DATASET,
        table=_table(SEC_COMPANY_FACTS_BRONZE_DATASET, _company_fact_rows()),
    )
    events: list[str] = []
    real_read_parquet = sec_postgres.read_parquet

    def read_with_event(location: BronzeRunLocation) -> pa.Table:
        events.append("read")
        return real_read_parquet(location)

    monkeypatch.setattr(sec_postgres, "read_parquet", read_with_event)
    connection, _, cursor = _mock_connection()
    _configure_combined_first_load(cursor)
    connection.cursor.side_effect = lambda: (
        events.append("cursor") or MagicMock(
            __enter__=MagicMock(return_value=cursor),
            __exit__=MagicMock(return_value=None),
        )
    )

    assert load_sec_company_bronze_to_postgres(
        connection,
        SecCompanyBronzeResult(_CIK, submissions, facts),
    ) == SecPostgresLoadResult(2, 2)

    assert events == ["read", "read", "cursor"]
    assert cursor.execute.call_args_list[2].args[0] == sec_postgres._INSERT_INGESTION_RUN
    assert cursor.executemany.call_args_list[0].args[0] == sec_postgres._INSERT_SEC_SUBMISSION
    assert cursor.execute.call_args_list[3].args[0] == sec_postgres._INSERT_INGESTION_RUN
    assert cursor.executemany.call_args_list[1].args[0] == sec_postgres._INSERT_SEC_COMPANY_FACT
    _assert_caller_owns_transaction(connection)


@pytest.mark.parametrize(
    ("submissions_rows", "facts_rows", "expected_calls"),
    [
        pytest.param([], _company_fact_rows(), 1, id="zero-submissions"),
        pytest.param(_submission_rows(), [], 1, id="zero-company-facts"),
        pytest.param([], [], 0, id="both-zero"),
    ],
)
def test_combined_load_registers_zero_row_datasets_without_row_inserts(
    tmp_path: Path,
    submissions_rows: list[dict[str, object]],
    facts_rows: list[dict[str, object]],
    expected_calls: int,
) -> None:
    submissions = _result(
        tmp_path,
        dataset=SEC_SUBMISSIONS_BRONZE_DATASET,
        table=_table(SEC_SUBMISSIONS_BRONZE_DATASET, submissions_rows),
    )
    facts = _result(
        tmp_path,
        dataset=SEC_COMPANY_FACTS_BRONZE_DATASET,
        table=_table(SEC_COMPANY_FACTS_BRONZE_DATASET, facts_rows),
    )
    connection, _, cursor = _mock_connection()
    _configure_combined_first_load(cursor)

    loaded = load_sec_company_bronze_to_postgres(
        connection,
        SecCompanyBronzeResult(_CIK, submissions, facts),
    )

    assert loaded == SecPostgresLoadResult(len(submissions_rows), len(facts_rows))
    assert cursor.execute.call_count == 4
    assert cursor.executemany.call_count == expected_calls


def test_combined_sec_replay_verifies_both_datasets_without_writes(
    tmp_path: Path,
) -> None:
    submissions = _result(
        tmp_path,
        dataset=SEC_SUBMISSIONS_BRONZE_DATASET,
        table=_table(SEC_SUBMISSIONS_BRONZE_DATASET, _submission_rows()),
    )
    facts = _result(
        tmp_path,
        dataset=SEC_COMPANY_FACTS_BRONZE_DATASET,
        table=_table(SEC_COMPANY_FACTS_BRONZE_DATASET, _company_fact_rows()),
    )
    connection, _, cursor = _mock_connection()
    cursor.fetchone.side_effect = [
        _registry_row(submissions),
        (submissions.record_count,),
        _registry_row(facts),
        (facts.record_count,),
    ]

    assert load_sec_company_bronze_to_postgres(
        connection,
        SecCompanyBronzeResult(_CIK, submissions, facts),
    ) == SecPostgresLoadResult(2, 2)

    assert cursor.execute.call_count == 4
    cursor.executemany.assert_not_called()


def test_combined_sec_rejects_mixed_committed_state_without_writes(
    tmp_path: Path,
) -> None:
    submissions = _result(tmp_path, dataset=SEC_SUBMISSIONS_BRONZE_DATASET)
    facts = _result(tmp_path, dataset=SEC_COMPANY_FACTS_BRONZE_DATASET)
    connection, _, cursor = _mock_connection()
    cursor.fetchone.side_effect = [_registry_row(submissions), (0,), None]

    with pytest.raises(SecPostgresLoadError, match="partially committed"):
        load_sec_company_bronze_to_postgres(
            connection,
            SecCompanyBronzeResult(_CIK, submissions, facts),
        )

    cursor.executemany.assert_not_called()


def test_combined_sec_rejects_inconsistent_committed_state_without_writes(
    tmp_path: Path,
) -> None:
    submissions = _result(tmp_path, dataset=SEC_SUBMISSIONS_BRONZE_DATASET)
    facts = _result(tmp_path, dataset=SEC_COMPANY_FACTS_BRONZE_DATASET)
    mismatched_submissions = list(_registry_row(submissions))
    mismatched_submissions[5] = "different/data.parquet"
    connection, _, cursor = _mock_connection()
    cursor.fetchone.side_effect = [tuple(mismatched_submissions)]

    with pytest.raises(SecPostgresLoadError, match="parquet_path"):
        load_sec_company_bronze_to_postgres(
            connection,
            SecCompanyBronzeResult(_CIK, submissions, facts),
        )

    cursor.executemany.assert_not_called()


@pytest.mark.parametrize(
    ("source", "dataset"),
    [
        pytest.param("other_source", SEC_SUBMISSIONS_BRONZE_DATASET, id="source"),
        pytest.param(SEC_BRONZE_SOURCE, "other_dataset", id="dataset"),
    ],
)
def test_rejects_wrong_identity_before_sql(
    tmp_path: Path,
    source: str,
    dataset: str,
) -> None:
    result = _result(tmp_path, source=source, dataset=dataset)
    connection, _, _ = _mock_connection()

    with pytest.raises(SecPostgresLoadError):
        load_sec_submissions_bronze_to_postgres(connection, result)

    connection.cursor.assert_not_called()


def test_rejects_non_normalized_cik_before_sql(tmp_path: Path) -> None:
    result = replace(
        _result(tmp_path, dataset=SEC_SUBMISSIONS_BRONZE_DATASET),
        cik="320193",
    )
    connection, _, _ = _mock_connection()

    with pytest.raises(SecPostgresLoadError, match="CIK"):
        load_sec_submissions_bronze_to_postgres(connection, result)

    connection.cursor.assert_not_called()


@pytest.mark.parametrize("attribute", ["raw_json_path", "parquet_path"])
def test_rejects_noncanonical_artifact_paths_before_sql(
    tmp_path: Path,
    attribute: str,
) -> None:
    result = _result(tmp_path, dataset=SEC_SUBMISSIONS_BRONZE_DATASET)
    result = replace(result, **{attribute: result.location.directory / "wrong"})
    connection, _, _ = _mock_connection()

    with pytest.raises(SecPostgresLoadError):
        load_sec_submissions_bronze_to_postgres(connection, result)

    connection.cursor.assert_not_called()


@pytest.mark.parametrize(
    "loader,dataset",
    [
        pytest.param(
            load_sec_submissions_bronze_to_postgres,
            SEC_SUBMISSIONS_BRONZE_DATASET,
            id="submissions",
        ),
        pytest.param(
            load_sec_company_facts_bronze_to_postgres,
            SEC_COMPANY_FACTS_BRONZE_DATASET,
            id="company-facts",
        ),
    ],
)
def test_rejects_schema_and_record_count_mismatches_before_sql(
    tmp_path: Path,
    loader: object,
    dataset: str,
) -> None:
    wrong_schema = pa.table({"cik": [_CIK]})
    schema_result = _result(tmp_path, dataset=dataset, table=wrong_schema)
    connection, _, _ = _mock_connection()

    with pytest.raises(SecPostgresLoadError, match="schema"):
        loader(connection, schema_result)  # type: ignore[operator]
    connection.cursor.assert_not_called()

    valid_result = _result(
        tmp_path / "count",
        dataset=dataset,
        table=_table(dataset, []),
    )
    count_result = replace(valid_result, record_count=1)
    with pytest.raises(SecPostgresLoadError, match="record count"):
        loader(connection, count_result)  # type: ignore[operator]
    connection.cursor.assert_not_called()


def test_rejects_parquet_row_cik_mismatch_before_sql(tmp_path: Path) -> None:
    rows = _submission_rows()
    rows[1]["cik"] = "0000789019"
    result = _result(
        tmp_path,
        dataset=SEC_SUBMISSIONS_BRONZE_DATASET,
        table=_table(SEC_SUBMISSIONS_BRONZE_DATASET, rows),
    )
    connection, _, _ = _mock_connection()

    with pytest.raises(SecPostgresLoadError, match="CIK"):
        load_sec_submissions_bronze_to_postgres(connection, result)

    connection.cursor.assert_not_called()


def test_combined_preflight_rejects_parent_cik_and_run_identity_mismatches(
    tmp_path: Path,
) -> None:
    submissions = _result(tmp_path, dataset=SEC_SUBMISSIONS_BRONZE_DATASET)
    facts = _result(tmp_path, dataset=SEC_COMPANY_FACTS_BRONZE_DATASET)
    connection, _, _ = _mock_connection()

    with pytest.raises(SecPostgresLoadError, match="CIK"):
        load_sec_company_bronze_to_postgres(
            connection,
            SecCompanyBronzeResult("0000789019", submissions, facts),
        )

    later_facts = _result(
        tmp_path / "later",
        dataset=SEC_COMPANY_FACTS_BRONZE_DATASET,
        run_at=_RUN_AT + timedelta(seconds=1),
    )
    with pytest.raises(SecPostgresLoadError, match="run identity"):
        load_sec_company_bronze_to_postgres(
            connection,
            SecCompanyBronzeResult(_CIK, submissions, later_facts),
        )
    connection.cursor.assert_not_called()


def test_combined_preflight_rejects_ingested_at_mismatch_with_equal_run_id(
    tmp_path: Path,
) -> None:
    submissions = _result(tmp_path, dataset=SEC_SUBMISSIONS_BRONZE_DATASET)
    facts = _result(
        tmp_path / "later",
        dataset=SEC_COMPANY_FACTS_BRONZE_DATASET,
        run_at=_RUN_AT + timedelta(seconds=1),
    )
    object.__setattr__(facts.location.metadata, "run_id", submissions.location.metadata.run_id)
    connection, _, _ = _mock_connection()

    with pytest.raises(SecPostgresLoadError, match="run identity"):
        load_sec_company_bronze_to_postgres(
            connection,
            SecCompanyBronzeResult(_CIK, submissions, facts),
        )

    connection.cursor.assert_not_called()


def test_missing_and_corrupt_parquet_errors_propagate_before_sql(
    tmp_path: Path,
) -> None:
    result = _result(tmp_path, dataset=SEC_SUBMISSIONS_BRONZE_DATASET)
    result.parquet_path.unlink()
    connection, _, _ = _mock_connection()

    with pytest.raises(FileNotFoundError):
        load_sec_submissions_bronze_to_postgres(connection, result)
    connection.cursor.assert_not_called()

    corrupt = _result(tmp_path / "corrupt", dataset=SEC_SUBMISSIONS_BRONZE_DATASET)
    corrupt.parquet_path.write_bytes(b"not a parquet artifact")
    with pytest.raises(BronzeParquetReadError):
        load_sec_submissions_bronze_to_postgres(connection, corrupt)
    connection.cursor.assert_not_called()


def test_database_failures_propagate_without_transaction_control(tmp_path: Path) -> None:
    submissions = _result(
        tmp_path,
        dataset=SEC_SUBMISSIONS_BRONZE_DATASET,
        table=_table(SEC_SUBMISSIONS_BRONZE_DATASET, _submission_rows()),
    )
    connection, _, cursor = _mock_connection()
    cursor.execute.side_effect = psycopg.OperationalError("submissions registry")

    with pytest.raises(psycopg.OperationalError, match="submissions registry"):
        load_sec_submissions_bronze_to_postgres(connection, submissions)
    cursor.executemany.assert_not_called()
    _assert_caller_owns_transaction(connection)

    connection, _, cursor = _mock_connection()
    cursor.executemany.side_effect = psycopg.OperationalError("submissions rows")
    with pytest.raises(psycopg.OperationalError, match="submissions rows"):
        load_sec_submissions_bronze_to_postgres(connection, submissions)
    _assert_caller_owns_transaction(connection)

    facts = _result(
        tmp_path / "combined",
        dataset=SEC_COMPANY_FACTS_BRONZE_DATASET,
        table=_table(SEC_COMPANY_FACTS_BRONZE_DATASET, _company_fact_rows()),
    )
    connection, _, cursor = _mock_connection()
    _configure_combined_first_load(cursor)
    cursor.executemany.side_effect = psycopg.OperationalError("combined submissions rows")
    with pytest.raises(psycopg.OperationalError, match="combined submissions rows"):
        load_sec_company_bronze_to_postgres(
            connection,
            SecCompanyBronzeResult(_CIK, submissions, facts),
        )
    assert cursor.execute.call_count == 4
    assert cursor.executemany.call_args.args[0] == sec_postgres._INSERT_SEC_SUBMISSION
    _assert_caller_owns_transaction(connection)


def test_combined_company_facts_failures_follow_submissions_sql(
    tmp_path: Path,
) -> None:
    submissions = _result(
        tmp_path,
        dataset=SEC_SUBMISSIONS_BRONZE_DATASET,
        table=_table(SEC_SUBMISSIONS_BRONZE_DATASET, _submission_rows()),
    )
    facts = _result(
        tmp_path,
        dataset=SEC_COMPANY_FACTS_BRONZE_DATASET,
        table=_table(SEC_COMPANY_FACTS_BRONZE_DATASET, _company_fact_rows()),
    )
    combined = SecCompanyBronzeResult(_CIK, submissions, facts)
    connection, _, cursor = _mock_connection()
    _configure_combined_first_load(cursor)
    cursor.execute.side_effect = [
        None,
        None,
        None,
        psycopg.OperationalError("facts registry"),
    ]

    with pytest.raises(psycopg.OperationalError, match="facts registry"):
        load_sec_company_bronze_to_postgres(connection, combined)
    cursor.executemany.assert_not_called()
    _assert_caller_owns_transaction(connection)

    connection, _, cursor = _mock_connection()
    _configure_combined_first_load(cursor)
    cursor.executemany.side_effect = [None, psycopg.OperationalError("facts rows")]
    with pytest.raises(psycopg.OperationalError, match="facts rows"):
        load_sec_company_bronze_to_postgres(connection, combined)
    assert cursor.execute.call_count == 4
    _assert_caller_owns_transaction(connection)


def test_loader_uses_exact_run_conflict_handling_without_provider_dependency() -> None:
    source = Path(sec_postgres.__file__).read_text(encoding="utf-8").upper()

    assert "ON CONFLICT (SOURCE, DATASET, RUN_ID) DO NOTHING" in source
    assert "MERGE" not in source
    assert "SECEDGARCLIENT" not in source


def test_sec_postgres_module_does_not_depend_on_market_or_fred() -> None:
    for value in vars(sec_postgres).values():
        referenced_module = (
            value.__name__
            if isinstance(value, ModuleType)
            else getattr(value, "__module__", "")
        )
        assert not referenced_module.startswith(("finstream.market", "finstream.fred"))
