from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock, call

import psycopg
import pyarrow as pa
import pytest

import finstream.market.postgres as market_postgres
from finstream.bronze.json_storage import write_raw_json
from finstream.bronze.models import BronzeRunLocation
from finstream.bronze.parquet_storage import BronzeParquetReadError, write_parquet
from finstream.market.bronze import (
    DAILY_MARKET_PRICE_SCHEMA,
    MARKET_BRONZE_DATASET,
    MARKET_BRONZE_SOURCE,
    MarketBronzeResult,
    daily_market_prices_to_table,
)
from finstream.market.models import DailyMarketPrice
from finstream.market.postgres import (
    MarketPostgresLoadError,
    load_market_bronze_to_postgres,
)


_RUN_AT = datetime(2026, 9, 18, 4, 5, 6, 123456, tzinfo=timezone.utc)


def _record(
    *,
    trading_date: date = date(2026, 9, 17),
    volume: int | None = 1234567,
) -> DailyMarketPrice:
    return DailyMarketPrice(
        symbol="AAPL",
        trading_date=trading_date,
        open=Decimal("150.100000000000000001"),
        high=Decimal("155.500000000000000001"),
        low=Decimal("149.250000000000000001"),
        close=Decimal("153.330000000000000001"),
        volume=volume,
    )


def _result(
    tmp_path: Path,
    *,
    source: str = MARKET_BRONZE_SOURCE,
    dataset: str = MARKET_BRONZE_DATASET,
    symbol: str = "AAPL",
    records: list[DailyMarketPrice] | None = None,
    table: pa.Table | None = None,
) -> MarketBronzeResult:
    location = BronzeRunLocation.from_run(
        root=tmp_path / "bronze",
        source=source,
        dataset=dataset,
        ingested_at=_RUN_AT,
    )
    write_raw_json(location, {"source": source, "dataset": dataset})
    persisted_table = (
        table
        if table is not None
        else daily_market_prices_to_table([] if records is None else records)
    )
    return MarketBronzeResult(
        symbol=symbol,
        location=location,
        raw_json_path=location.directory / "payload.json",
        parquet_path=write_parquet(location, persisted_table),
        record_count=persisted_table.num_rows,
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


def test_loads_non_empty_market_bronze_in_source_order_with_decimal_values(
    tmp_path: Path,
) -> None:
    result = _result(
        tmp_path,
        records=[
            _record(trading_date=date(2026, 9, 17)),
            _record(trading_date=date(2026, 9, 16), volume=None),
        ],
    )
    connection, cursor_context, cursor = _mock_connection()

    assert load_market_bronze_to_postgres(connection, result) == 2

    connection.cursor.assert_called_once_with()
    cursor_context.__enter__.assert_called_once_with()
    cursor_context.__exit__.assert_called_once_with(None, None, None)
    registry_sql, registry_parameters = cursor.execute.call_args.args
    assert "INSERT INTO source_data.ingestion_runs" in registry_sql
    assert "loaded_at" not in registry_sql
    assert registry_parameters == (
        MARKET_BRONZE_SOURCE,
        MARKET_BRONZE_DATASET,
        result.location.metadata.run_id,
        _RUN_AT,
        str(result.raw_json_path),
        str(result.parquet_path),
        2,
    )
    market_sql, rows = cursor.executemany.call_args.args
    rows = list(rows)
    assert "INSERT INTO source_data.market_daily_prices" in market_sql
    assert "%s" in market_sql
    assert rows == [
        (
            MARKET_BRONZE_SOURCE,
            MARKET_BRONZE_DATASET,
            result.location.metadata.run_id,
            0,
            "AAPL",
            date(2026, 9, 17),
            Decimal("150.100000000000000001"),
            Decimal("155.500000000000000001"),
            Decimal("149.250000000000000001"),
            Decimal("153.330000000000000001"),
            1234567,
        ),
        (
            MARKET_BRONZE_SOURCE,
            MARKET_BRONZE_DATASET,
            result.location.metadata.run_id,
            1,
            "AAPL",
            date(2026, 9, 16),
            Decimal("150.100000000000000001"),
            Decimal("155.500000000000000001"),
            Decimal("149.250000000000000001"),
            Decimal("153.330000000000000001"),
            None,
        ),
    ]
    assert all(isinstance(row[6], Decimal) for row in rows)
    _assert_caller_owns_transaction(connection)


def test_loads_valid_zero_row_run_without_market_executemany(tmp_path: Path) -> None:
    result = _result(tmp_path, records=[])
    connection, _, cursor = _mock_connection()

    assert load_market_bronze_to_postgres(connection, result) == 0

    assert cursor.execute.call_count == 1
    assert cursor.execute.call_args.args[1][-1] == 0
    cursor.executemany.assert_not_called()
    _assert_caller_owns_transaction(connection)


@pytest.mark.parametrize(
    ("source", "dataset"),
    [
        pytest.param("other_source", MARKET_BRONZE_DATASET, id="wrong-source"),
        pytest.param(MARKET_BRONZE_SOURCE, "other_dataset", id="wrong-dataset"),
    ],
)
def test_rejects_noncanonical_market_identity_before_sql(
    tmp_path: Path,
    source: str,
    dataset: str,
) -> None:
    result = _result(tmp_path, source=source, dataset=dataset, records=[_record()])
    connection, _, _ = _mock_connection()

    with pytest.raises(MarketPostgresLoadError):
        load_market_bronze_to_postgres(connection, result)

    connection.cursor.assert_not_called()


@pytest.mark.parametrize("path_name", ["raw_json_path", "parquet_path"])
def test_rejects_noncanonical_artifact_paths_before_sql(
    tmp_path: Path,
    path_name: str,
) -> None:
    result = _result(tmp_path, records=[_record()])
    replacement = result.location.directory / f"unexpected-{path_name}"
    result = MarketBronzeResult(
        **{**result.__dict__, path_name: replacement},
    )
    connection, _, _ = _mock_connection()

    with pytest.raises(MarketPostgresLoadError):
        load_market_bronze_to_postgres(connection, result)

    connection.cursor.assert_not_called()


def test_rejects_wrong_parquet_schema_before_sql(tmp_path: Path) -> None:
    wrong_schema_table = pa.table({"symbol": ["AAPL"]})
    result = _result(tmp_path, table=wrong_schema_table)
    connection, _, _ = _mock_connection()

    with pytest.raises(MarketPostgresLoadError, match="schema"):
        load_market_bronze_to_postgres(connection, result)

    connection.cursor.assert_not_called()


def test_rejects_record_count_mismatch_before_sql(tmp_path: Path) -> None:
    result = _result(tmp_path, records=[_record()])
    result = MarketBronzeResult(**{**result.__dict__, "record_count": 2})
    connection, _, _ = _mock_connection()

    with pytest.raises(MarketPostgresLoadError, match="record count"):
        load_market_bronze_to_postgres(connection, result)

    connection.cursor.assert_not_called()


def test_rejects_symbol_mismatch_before_sql(tmp_path: Path) -> None:
    result = _result(tmp_path, records=[_record()])
    result = MarketBronzeResult(**{**result.__dict__, "symbol": "MSFT"})
    connection, _, _ = _mock_connection()

    with pytest.raises(MarketPostgresLoadError, match="symbols"):
        load_market_bronze_to_postgres(connection, result)

    connection.cursor.assert_not_called()


def test_missing_parquet_error_propagates_without_database_access(tmp_path: Path) -> None:
    location = BronzeRunLocation.from_run(
        root=tmp_path / "bronze",
        source=MARKET_BRONZE_SOURCE,
        dataset=MARKET_BRONZE_DATASET,
        ingested_at=_RUN_AT,
    )
    result = MarketBronzeResult(
        symbol="AAPL",
        location=location,
        raw_json_path=location.directory / "payload.json",
        parquet_path=location.directory / "data.parquet",
        record_count=0,
    )
    connection, _, _ = _mock_connection()

    with pytest.raises(FileNotFoundError):
        load_market_bronze_to_postgres(connection, result)

    connection.cursor.assert_not_called()


def test_corrupt_parquet_error_propagates_without_database_access(
    tmp_path: Path,
) -> None:
    result = _result(tmp_path, records=[_record()])
    result.parquet_path.write_bytes(b"not a parquet artifact")
    connection, _, _ = _mock_connection()

    with pytest.raises(BronzeParquetReadError):
        load_market_bronze_to_postgres(connection, result)

    connection.cursor.assert_not_called()


def test_registry_database_error_propagates_without_market_loading(
    tmp_path: Path,
) -> None:
    result = _result(tmp_path, records=[_record()])
    connection, cursor_context, cursor = _mock_connection()
    cursor.execute.side_effect = psycopg.OperationalError("registry unavailable")

    with pytest.raises(psycopg.OperationalError, match="registry unavailable"):
        load_market_bronze_to_postgres(connection, result)

    cursor.executemany.assert_not_called()
    assert cursor_context.__exit__.call_count == 1
    _assert_caller_owns_transaction(connection)


def test_market_database_error_propagates_with_registry_already_inserted(
    tmp_path: Path,
) -> None:
    result = _result(tmp_path, records=[_record()])
    connection, _, cursor = _mock_connection()
    cursor.executemany.side_effect = psycopg.OperationalError("market unavailable")

    with pytest.raises(psycopg.OperationalError, match="market unavailable"):
        load_market_bronze_to_postgres(connection, result)

    assert cursor.execute.call_args_list == [
        call(
            market_postgres._INSERT_INGESTION_RUN,
            (
                MARKET_BRONZE_SOURCE,
                MARKET_BRONZE_DATASET,
                result.location.metadata.run_id,
                _RUN_AT,
                str(result.raw_json_path),
                str(result.parquet_path),
                1,
            ),
        )
    ]
    _assert_caller_owns_transaction(connection)


def test_loader_has_no_application_level_duplicate_run_policy() -> None:
    source = Path(market_postgres.__file__).read_text(encoding="utf-8").upper()

    assert "ON CONFLICT" not in source
    assert "MERGE" not in source
    assert "SELECT" not in source


def test_market_postgres_module_avoids_other_provider_dependencies() -> None:
    for value in vars(market_postgres).values():
        referenced_module = (
            value.__name__
            if isinstance(value, ModuleType)
            else getattr(value, "__module__", "")
        )
        assert not referenced_module.startswith(("finstream.sec", "finstream.fred"))


def test_schema_constant_remains_exact_for_real_bronze_artifacts(tmp_path: Path) -> None:
    result = _result(tmp_path, records=[_record()])

    assert result.record_count == 1
    assert DAILY_MARKET_PRICE_SCHEMA.equals(
        pa.parquet.ParquetFile(result.parquet_path).read().schema,
        check_metadata=True,
    )
