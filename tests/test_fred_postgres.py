from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock

import psycopg
import pyarrow as pa
import pytest

import finstream.fred.postgres as fred_postgres
from finstream.bronze.json_storage import write_raw_json
from finstream.bronze.models import BronzeRunLocation
from finstream.bronze.parquet_storage import BronzeParquetReadError, write_parquet
from finstream.fred.bronze import (
    FRED_BRONZE_SOURCE,
    FRED_SERIES_METADATA_BRONZE_DATASET,
    FRED_SERIES_METADATA_SCHEMA,
    FRED_SERIES_OBSERVATIONS_BRONZE_DATASET,
    FRED_SERIES_OBSERVATIONS_SCHEMA,
    FredBronzeDatasetResult,
    FredSeriesBronzeResult,
)
from finstream.fred.postgres import (
    FredPostgresLoadError,
    FredPostgresLoadResult,
    load_fred_metadata_bronze_to_postgres,
    load_fred_observations_bronze_to_postgres,
    load_fred_series_bronze_to_postgres,
)


_SERIES_ID = "DFF"
_RUN_AT = datetime(2026, 9, 18, 4, 5, 6, 123456, tzinfo=timezone.utc)


def _metadata_row() -> dict[str, object]:
    return {
        "series_id": _SERIES_ID,
        "realtime_start": date(2026, 8, 1),
        "realtime_end": date(2026, 8, 29),
        "title": "Federal Funds Effective Rate",
        "observation_start": date(1954, 7, 1),
        "observation_end": date(2026, 8, 28),
        "frequency": "Daily",
        "frequency_short": "D",
        "units": "Percent",
        "units_short": "%",
        "seasonal_adjustment": "Not Seasonally Adjusted",
        "seasonal_adjustment_short": "NSA",
        "last_updated": datetime(
            2026, 8, 29, 19, 0, 0, tzinfo=timezone(timedelta(hours=7))
        ),
        "popularity": 99,
        "notes": None,
    }


def _observation_rows() -> list[dict[str, object]]:
    return [
        {
            "series_id": _SERIES_ID,
            "realtime_start": date(2026, 8, 29),
            "realtime_end": date(2026, 8, 29),
            "observation_date": date(2026, 8, 28),
            "value": Decimal("4.330000000000000000000000000000"),
        },
        {
            "series_id": _SERIES_ID,
            "realtime_start": date(2026, 8, 1),
            "realtime_end": date(2026, 8, 29),
            "observation_date": date(2026, 8, 27),
            "value": None,
        },
    ]


def _table(dataset: str, rows: list[dict[str, object]] | None = None) -> pa.Table:
    schema = (
        FRED_SERIES_METADATA_SCHEMA
        if dataset == FRED_SERIES_METADATA_BRONZE_DATASET
        else FRED_SERIES_OBSERVATIONS_SCHEMA
    )
    return pa.Table.from_pylist([] if rows is None else rows, schema=schema)


def _result(
    tmp_path: Path,
    *,
    dataset: str,
    table: pa.Table | None = None,
    source: str = FRED_BRONZE_SOURCE,
    series_id: str = _SERIES_ID,
    run_at: datetime = _RUN_AT,
) -> FredBronzeDatasetResult:
    location = BronzeRunLocation.from_run(
        root=tmp_path / "bronze",
        source=source,
        dataset=dataset,
        ingested_at=run_at,
    )
    write_raw_json(location, {"source": source, "dataset": dataset})
    if table is None:
        rows = [_metadata_row()] if dataset == FRED_SERIES_METADATA_BRONZE_DATASET else []
        table = _table(dataset, rows)
    return FredBronzeDatasetResult(
        series_id=series_id,
        location=location,
        raw_json_path=location.directory / "payload.json",
        parquet_path=write_parquet(location, table),
        record_count=table.num_rows,
    )


def _mock_connection() -> tuple[MagicMock, MagicMock, MagicMock]:
    connection = MagicMock(spec=psycopg.Connection)
    cursor_context = connection.cursor.return_value
    cursor = cursor_context.__enter__.return_value
    return connection, cursor_context, cursor


def _assert_caller_owns_transaction(connection: MagicMock) -> None:
    connection.commit.assert_not_called()
    connection.rollback.assert_not_called()
    connection.close.assert_not_called()


def test_loads_exactly_one_metadata_row_with_timezone_and_nulls(tmp_path: Path) -> None:
    result = _result(tmp_path, dataset=FRED_SERIES_METADATA_BRONZE_DATASET)
    connection, cursor_context, cursor = _mock_connection()

    assert load_fred_metadata_bronze_to_postgres(connection, result) == 1

    connection.cursor.assert_called_once_with()
    cursor_context.__enter__.assert_called_once_with()
    registry_sql, registry_values = cursor.execute.call_args_list[0].args
    row_sql, row_values = cursor.execute.call_args_list[1].args
    assert "INSERT INTO source_data.ingestion_runs" in registry_sql
    assert "loaded_at" not in registry_sql
    assert registry_values[-1] == 1
    assert "INSERT INTO source_data.fred_series_metadata" in row_sql
    assert "%s" in row_sql
    assert row_values[3:10] == (
        0,
        _SERIES_ID,
        date(2026, 8, 1),
        date(2026, 8, 29),
        "Federal Funds Effective Rate",
        date(1954, 7, 1),
        date(2026, 8, 28),
    )
    assert row_values[16] == datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc)
    assert row_values[-1] is None
    cursor.executemany.assert_not_called()
    _assert_caller_owns_transaction(connection)


@pytest.mark.parametrize(
    "rows",
    [
        pytest.param([], id="zero"),
        pytest.param([_metadata_row(), _metadata_row()], id="more-than-one"),
    ],
)
def test_rejects_metadata_without_exactly_one_row_before_sql(
    tmp_path: Path,
    rows: list[dict[str, object]],
) -> None:
    result = _result(
        tmp_path,
        dataset=FRED_SERIES_METADATA_BRONZE_DATASET,
        table=_table(FRED_SERIES_METADATA_BRONZE_DATASET, rows),
    )
    connection, _, _ = _mock_connection()

    with pytest.raises(FredPostgresLoadError, match="exactly one"):
        load_fred_metadata_bronze_to_postgres(connection, result)

    connection.cursor.assert_not_called()


def test_loads_observations_in_order_preserving_decimal_null_and_realtime(
    tmp_path: Path,
) -> None:
    result = _result(
        tmp_path,
        dataset=FRED_SERIES_OBSERVATIONS_BRONZE_DATASET,
        table=_table(FRED_SERIES_OBSERVATIONS_BRONZE_DATASET, _observation_rows()),
    )
    connection, _, cursor = _mock_connection()

    assert load_fred_observations_bronze_to_postgres(connection, result) == 2

    rows_sql, rows = cursor.executemany.call_args.args
    rows = list(rows)
    assert "INSERT INTO source_data.fred_series_observations" in rows_sql
    assert "%s" in rows_sql
    assert [row[3] for row in rows] == [0, 1]
    assert [row[7] for row in rows] == [date(2026, 8, 28), date(2026, 8, 27)]
    assert isinstance(rows[0][8], Decimal)
    assert rows[1][8] is None
    assert rows[1][5:7] == (date(2026, 8, 1), date(2026, 8, 29))
    _assert_caller_owns_transaction(connection)


def test_loads_zero_row_observations_registry_only(tmp_path: Path) -> None:
    result = _result(tmp_path, dataset=FRED_SERIES_OBSERVATIONS_BRONZE_DATASET)
    connection, _, cursor = _mock_connection()

    assert load_fred_observations_bronze_to_postgres(connection, result) == 0

    assert cursor.execute.call_args.args[1][-1] == 0
    cursor.executemany.assert_not_called()


@pytest.mark.parametrize(
    ("source", "dataset"),
    [
        pytest.param("other_source", FRED_SERIES_METADATA_BRONZE_DATASET, id="source"),
        pytest.param(FRED_BRONZE_SOURCE, "other_dataset", id="dataset"),
    ],
)
def test_rejects_wrong_metadata_identity_before_sql(
    tmp_path: Path,
    source: str,
    dataset: str,
) -> None:
    result = _result(tmp_path, source=source, dataset=dataset)
    connection, _, _ = _mock_connection()

    with pytest.raises(FredPostgresLoadError):
        load_fred_metadata_bronze_to_postgres(connection, result)

    connection.cursor.assert_not_called()


@pytest.mark.parametrize(
    ("source", "dataset"),
    [
        pytest.param("other_source", FRED_SERIES_OBSERVATIONS_BRONZE_DATASET, id="source"),
        pytest.param(FRED_BRONZE_SOURCE, "other_dataset", id="dataset"),
    ],
)
def test_rejects_wrong_observations_identity_before_sql(
    tmp_path: Path,
    source: str,
    dataset: str,
) -> None:
    result = _result(tmp_path, source=source, dataset=dataset)
    connection, _, _ = _mock_connection()

    with pytest.raises(FredPostgresLoadError):
        load_fred_observations_bronze_to_postgres(connection, result)

    connection.cursor.assert_not_called()


@pytest.mark.parametrize("attribute", ["raw_json_path", "parquet_path"])
def test_rejects_invalid_series_id_and_noncanonical_paths_before_sql(
    tmp_path: Path,
    attribute: str,
) -> None:
    result = _result(tmp_path, dataset=FRED_SERIES_OBSERVATIONS_BRONZE_DATASET)
    connection, _, _ = _mock_connection()
    with pytest.raises(FredPostgresLoadError):
        load_fred_observations_bronze_to_postgres(
            connection,
            replace(result, series_id=" DFF "),
        )
    with pytest.raises(FredPostgresLoadError):
        load_fred_observations_bronze_to_postgres(
            connection,
            replace(result, **{attribute: result.location.directory / "wrong"}),
        )
    connection.cursor.assert_not_called()


@pytest.mark.parametrize("attribute", ["raw_json_path", "parquet_path"])
def test_rejects_noncanonical_metadata_paths_before_sql(
    tmp_path: Path,
    attribute: str,
) -> None:
    result = _result(tmp_path, dataset=FRED_SERIES_METADATA_BRONZE_DATASET)
    connection, _, _ = _mock_connection()

    with pytest.raises(FredPostgresLoadError):
        load_fred_metadata_bronze_to_postgres(
            connection,
            replace(result, **{attribute: result.location.directory / "wrong"}),
        )

    connection.cursor.assert_not_called()


@pytest.mark.parametrize(
    "loader,dataset,rows",
    [
        pytest.param(
            load_fred_metadata_bronze_to_postgres,
            FRED_SERIES_METADATA_BRONZE_DATASET,
            [_metadata_row()],
            id="metadata",
        ),
        pytest.param(
            load_fred_observations_bronze_to_postgres,
            FRED_SERIES_OBSERVATIONS_BRONZE_DATASET,
            _observation_rows(),
            id="observations",
        ),
    ],
)
def test_rejects_schema_count_and_row_series_id_mismatches_before_sql(
    tmp_path: Path,
    loader: object,
    dataset: str,
    rows: list[dict[str, object]],
) -> None:
    connection, _, _ = _mock_connection()
    wrong_schema = _result(tmp_path, dataset=dataset, table=pa.table({"series_id": [_SERIES_ID]}))
    with pytest.raises(FredPostgresLoadError, match="schema"):
        loader(connection, wrong_schema)  # type: ignore[operator]

    valid = _result(tmp_path / "count", dataset=dataset, table=_table(dataset, rows))
    with pytest.raises(FredPostgresLoadError, match="record count"):
        loader(connection, replace(valid, record_count=valid.record_count + 1))  # type: ignore[operator]

    mismatched_rows = [dict(row) for row in rows]
    mismatched_rows[0]["series_id"] = "OTHER"
    mismatch = _result(
        tmp_path / "series",
        dataset=dataset,
        table=_table(dataset, mismatched_rows),
    )
    with pytest.raises(FredPostgresLoadError, match="series IDs"):
        loader(connection, mismatch)  # type: ignore[operator]
    connection.cursor.assert_not_called()


def test_missing_and_corrupt_parquet_errors_propagate_before_sql(
    tmp_path: Path,
) -> None:
    missing = _result(tmp_path, dataset=FRED_SERIES_OBSERVATIONS_BRONZE_DATASET)
    missing.parquet_path.unlink()
    connection, _, _ = _mock_connection()

    with pytest.raises(FileNotFoundError):
        load_fred_observations_bronze_to_postgres(connection, missing)
    connection.cursor.assert_not_called()

    corrupt = _result(
        tmp_path / "corrupt",
        dataset=FRED_SERIES_OBSERVATIONS_BRONZE_DATASET,
    )
    corrupt.parquet_path.write_bytes(b"not a parquet artifact")
    with pytest.raises(BronzeParquetReadError):
        load_fred_observations_bronze_to_postgres(connection, corrupt)
    connection.cursor.assert_not_called()


def test_combined_load_preflights_both_then_uses_one_cursor_in_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    metadata = _result(tmp_path, dataset=FRED_SERIES_METADATA_BRONZE_DATASET)
    observations = _result(
        tmp_path,
        dataset=FRED_SERIES_OBSERVATIONS_BRONZE_DATASET,
        table=_table(FRED_SERIES_OBSERVATIONS_BRONZE_DATASET, _observation_rows()),
    )
    events: list[str] = []
    real_read_parquet = fred_postgres.read_parquet

    def read_with_event(location: BronzeRunLocation) -> pa.Table:
        events.append("read")
        return real_read_parquet(location)

    monkeypatch.setattr(fred_postgres, "read_parquet", read_with_event)
    connection, _, cursor = _mock_connection()
    connection.cursor.side_effect = lambda: (
        events.append("cursor") or MagicMock(
            __enter__=MagicMock(return_value=cursor),
            __exit__=MagicMock(return_value=None),
        )
    )

    assert load_fred_series_bronze_to_postgres(
        connection,
        FredSeriesBronzeResult(_SERIES_ID, metadata, observations),
    ) == FredPostgresLoadResult(1, 2)

    assert events == ["read", "read", "cursor"]
    assert [entry.args[0] for entry in cursor.execute.call_args_list] == [
        fred_postgres._INSERT_INGESTION_RUN,
        fred_postgres._INSERT_FRED_METADATA,
        fred_postgres._INSERT_INGESTION_RUN,
    ]
    assert cursor.executemany.call_args.args[0] == fred_postgres._INSERT_FRED_OBSERVATION
    _assert_caller_owns_transaction(connection)


def test_combined_load_with_zero_observations_registers_both_datasets(
    tmp_path: Path,
) -> None:
    metadata = _result(tmp_path, dataset=FRED_SERIES_METADATA_BRONZE_DATASET)
    observations = _result(tmp_path, dataset=FRED_SERIES_OBSERVATIONS_BRONZE_DATASET)
    connection, _, cursor = _mock_connection()

    assert load_fred_series_bronze_to_postgres(
        connection,
        FredSeriesBronzeResult(_SERIES_ID, metadata, observations),
    ) == FredPostgresLoadResult(1, 0)

    assert cursor.execute.call_args_list[2].args[1][-1] == 0
    cursor.executemany.assert_not_called()


def test_combined_preflight_rejects_series_and_run_identity_before_sql(
    tmp_path: Path,
) -> None:
    metadata = _result(tmp_path, dataset=FRED_SERIES_METADATA_BRONZE_DATASET)
    observations = _result(tmp_path, dataset=FRED_SERIES_OBSERVATIONS_BRONZE_DATASET)
    connection, _, _ = _mock_connection()

    with pytest.raises(FredPostgresLoadError, match="series IDs"):
        load_fred_series_bronze_to_postgres(
            connection,
            FredSeriesBronzeResult("OTHER", metadata, observations),
        )

    later = _result(
        tmp_path / "later",
        dataset=FRED_SERIES_OBSERVATIONS_BRONZE_DATASET,
        run_at=_RUN_AT + timedelta(seconds=1),
    )
    with pytest.raises(FredPostgresLoadError, match="run identity"):
        load_fred_series_bronze_to_postgres(
            connection,
            FredSeriesBronzeResult(_SERIES_ID, metadata, later),
        )

    object.__setattr__(later.location.metadata, "run_id", metadata.location.metadata.run_id)
    with pytest.raises(FredPostgresLoadError, match="run identity"):
        load_fred_series_bronze_to_postgres(
            connection,
            FredSeriesBronzeResult(_SERIES_ID, metadata, later),
        )
    connection.cursor.assert_not_called()


def test_database_failures_propagate_without_transaction_control(tmp_path: Path) -> None:
    metadata = _result(tmp_path, dataset=FRED_SERIES_METADATA_BRONZE_DATASET)
    observations = _result(
        tmp_path,
        dataset=FRED_SERIES_OBSERVATIONS_BRONZE_DATASET,
        table=_table(FRED_SERIES_OBSERVATIONS_BRONZE_DATASET, _observation_rows()),
    )
    connection, _, cursor = _mock_connection()
    cursor.execute.side_effect = psycopg.OperationalError("metadata registry")
    with pytest.raises(psycopg.OperationalError, match="metadata registry"):
        load_fred_metadata_bronze_to_postgres(connection, metadata)
    _assert_caller_owns_transaction(connection)

    connection, _, cursor = _mock_connection()
    cursor.execute.side_effect = [None, psycopg.OperationalError("metadata row")]
    with pytest.raises(psycopg.OperationalError, match="metadata row"):
        load_fred_series_bronze_to_postgres(
            connection,
            FredSeriesBronzeResult(_SERIES_ID, metadata, observations),
        )
    assert cursor.execute.call_count == 2
    cursor.executemany.assert_not_called()
    _assert_caller_owns_transaction(connection)

    connection, _, cursor = _mock_connection()
    cursor.execute.side_effect = [None, None, psycopg.OperationalError("observations registry")]
    with pytest.raises(psycopg.OperationalError, match="observations registry"):
        load_fred_series_bronze_to_postgres(
            connection,
            FredSeriesBronzeResult(_SERIES_ID, metadata, observations),
        )
    cursor.executemany.assert_not_called()
    _assert_caller_owns_transaction(connection)

    connection, _, cursor = _mock_connection()
    cursor.executemany.side_effect = psycopg.OperationalError("observation rows")
    with pytest.raises(psycopg.OperationalError, match="observation rows"):
        load_fred_series_bronze_to_postgres(
            connection,
            FredSeriesBronzeResult(_SERIES_ID, metadata, observations),
        )
    assert cursor.execute.call_count == 3
    _assert_caller_owns_transaction(connection)


def test_loader_has_no_duplicate_or_provider_policy() -> None:
    source = Path(fred_postgres.__file__).read_text(encoding="utf-8").upper()

    assert "ON CONFLICT" not in source
    assert "MERGE" not in source
    assert "SELECT" not in source
    assert "FREDCLIENT" not in source


def test_fred_postgres_module_does_not_depend_on_market_or_sec() -> None:
    for value in vars(fred_postgres).values():
        referenced_module = (
            value.__name__
            if isinstance(value, ModuleType)
            else getattr(value, "__module__", "")
        )
        assert not referenced_module.startswith(("finstream.market", "finstream.sec"))
