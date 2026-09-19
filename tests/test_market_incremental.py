from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, Mock

import psycopg
import pytest

from finstream.bronze.parquet_storage import read_parquet
from finstream.market.ingestion import (
    MARKET_INCREMENTAL_OVERLAP_DAYS,
    MarketIngestionService,
    market_incremental_start_date,
)
from finstream.market.postgres import latest_market_trading_date
from finstream.market.twelve_data import TwelveDataClient


_RUN_AT = datetime(2026, 9, 19, 12, 0, 0, tzinfo=timezone.utc)


def _payload(symbol: str, values: list[dict[str, str]] | None = None) -> dict:
    return {
        "meta": {"symbol": symbol, "interval": "1day"},
        "values": [] if values is None else values,
    }


def _row(trading_date: str, *, close: str) -> dict[str, str]:
    return {
        "datetime": trading_date,
        "open": "250",
        "high": "255",
        "low": "245",
        "close": close,
        "volume": "100",
    }


def _watermark_connection(
    latest_trading_date: date | None,
) -> tuple[MagicMock, MagicMock]:
    connection = MagicMock(spec=psycopg.Connection)
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = (latest_trading_date,)
    return connection, cursor


def test_market_watermark_returns_none_when_symbol_has_no_history() -> None:
    connection, cursor = _watermark_connection(None)

    assert latest_market_trading_date(connection, " aapl ") is None

    cursor.execute.assert_called_once_with(
        """
    SELECT max(trading_date)
    FROM source_data.market_daily_prices
    WHERE source = %s AND dataset = %s AND symbol = %s
""",
        ("twelve_data", "daily_market_prices", "AAPL"),
    )
    connection.commit.assert_not_called()
    connection.rollback.assert_not_called()
    connection.close.assert_not_called()


def test_market_watermark_returns_exact_maximum_for_requested_symbol_only() -> None:
    connection, cursor = _watermark_connection(date(2026, 9, 18))

    assert latest_market_trading_date(connection, "MSFT") == date(2026, 9, 18)

    assert cursor.execute.call_args.args[1] == (
        "twelve_data",
        "daily_market_prices",
        "MSFT",
    )
    connection.commit.assert_not_called()
    connection.rollback.assert_not_called()
    connection.close.assert_not_called()


@pytest.mark.parametrize(
    ("latest_trading_date", "overlap_days", "expected"),
    [
        (None, MARKET_INCREMENTAL_OVERLAP_DAYS, None),
        (date(2026, 9, 18), 3, date(2026, 9, 15)),
        (date(2026, 9, 18), 0, date(2026, 9, 18)),
    ],
)
def test_market_incremental_start_date_uses_calendar_day_arithmetic(
    latest_trading_date: date | None,
    overlap_days: int,
    expected: date | None,
) -> None:
    assert (
        market_incremental_start_date(
            latest_trading_date,
            overlap_days=overlap_days,
        )
        == expected
    )


@pytest.mark.parametrize("overlap_days", [-1, True, "3"])
def test_market_incremental_start_date_rejects_invalid_overlap(
    overlap_days: object,
) -> None:
    with pytest.raises(ValueError):
        market_incremental_start_date(
            date(2026, 9, 18),
            overlap_days=overlap_days,  # type: ignore[arg-type]
        )


def test_incremental_market_bootstrap_preserves_unbounded_fetch(
    tmp_path: Path,
) -> None:
    connection, _ = _watermark_connection(None)
    client = Mock(spec=TwelveDataClient)
    client.fetch_daily_time_series.return_value = _payload(
        "AAPL",
        [_row("2026-09-18", close="250")],
    )

    result = MarketIngestionService(client).ingest_symbol_incrementally_to_bronze(
        connection,
        " aapl ",
        run_at=_RUN_AT,
        bronze_root=tmp_path / "bronze",
    )

    assert result.record_count == 1
    client.fetch_daily_time_series.assert_called_once_with(
        "AAPL",
        start_date=None,
        end_date=None,
    )
    client.fetch_daily_time_series_since.assert_not_called()


def test_incremental_market_uses_postgres_watermark_and_overlap(
    tmp_path: Path,
) -> None:
    connection, _ = _watermark_connection(date(2026, 9, 18))
    client = Mock(spec=TwelveDataClient)
    client.fetch_daily_time_series_since.return_value = _payload(
        "AAPL",
        [_row("2026-09-18", close="252")],
    )

    result = MarketIngestionService(client).ingest_symbol_incrementally_to_bronze(
        connection,
        "AAPL",
        run_at=_RUN_AT,
        bronze_root=tmp_path / "bronze",
    )

    assert result.record_count == 1
    client.fetch_daily_time_series_since.assert_called_once_with(
        "AAPL",
        start_date=date(2026, 9, 15),
    )
    client.fetch_daily_time_series.assert_not_called()


def test_explicit_market_range_overrides_automatic_watermark(
    tmp_path: Path,
) -> None:
    connection = MagicMock(spec=psycopg.Connection)
    client = Mock(spec=TwelveDataClient)
    client.fetch_daily_time_series.return_value = _payload(
        "AAPL",
        [_row("2026-08-18", close="250")],
    )

    result = MarketIngestionService(client).ingest_symbol_incrementally_to_bronze(
        connection,
        "AAPL",
        run_at=_RUN_AT,
        bronze_root=tmp_path / "bronze",
        start_date=date(2026, 8, 1),
        end_date=date(2026, 8, 31),
    )

    assert result.record_count == 1
    connection.cursor.assert_not_called()
    client.fetch_daily_time_series.assert_called_once_with(
        "AAPL",
        start_date=date(2026, 8, 1),
        end_date=date(2026, 8, 31),
    )
    client.fetch_daily_time_series_since.assert_not_called()


def test_incremental_market_preserves_explicit_range_validation_before_query(
    tmp_path: Path,
) -> None:
    connection = MagicMock(spec=psycopg.Connection)
    client = Mock(spec=TwelveDataClient)

    with pytest.raises(ValueError, match="supplied together"):
        MarketIngestionService(client).ingest_symbol_incrementally_to_bronze(
            connection,
            "AAPL",
            run_at=_RUN_AT,
            bronze_root=tmp_path / "bronze",
            start_date=date(2026, 9, 1),
        )

    connection.cursor.assert_not_called()
    client.fetch_daily_time_series.assert_not_called()
    client.fetch_daily_time_series_since.assert_not_called()


def test_incremental_market_retains_overlap_correction_in_new_bronze_run(
    tmp_path: Path,
) -> None:
    connection, _ = _watermark_connection(date(2026, 9, 18))
    client = Mock(spec=TwelveDataClient)
    client.fetch_daily_time_series_since.return_value = _payload(
        "AAPL",
        [
            _row("2026-09-19", close="253"),
            _row("2026-09-18", close="252"),
        ],
    )

    result = MarketIngestionService(client).ingest_symbol_incrementally_to_bronze(
        connection,
        "AAPL",
        run_at=_RUN_AT,
        bronze_root=tmp_path / "bronze",
    )

    table = read_parquet(result.location)
    assert result.record_count == 2
    assert table.column("trading_date").to_pylist() == [
        date(2026, 9, 19),
        date(2026, 9, 18),
    ]
    assert table.column("close").to_pylist() == [Decimal("253"), Decimal("252")]


def test_incremental_market_runs_compose_with_same_run_reuse_and_correction(
    tmp_path: Path,
) -> None:
    run_a_at = _RUN_AT - timedelta(days=1)
    bootstrap_connection, _ = _watermark_connection(None)
    bootstrap_client = Mock(spec=TwelveDataClient)
    bootstrap_client.fetch_daily_time_series.return_value = _payload(
        "AAPL",
        [
            _row("2026-09-17", close="249"),
            _row("2026-09-18", close="250"),
        ],
    )
    service = MarketIngestionService(bootstrap_client)

    run_a = service.ingest_symbol_incrementally_to_bronze(
        bootstrap_connection, "AAPL", run_at=run_a_at, bronze_root=tmp_path / "bronze"
    )
    reused_run_a = service.ingest_symbol_incrementally_to_bronze(
        bootstrap_connection, "AAPL", run_at=run_a_at, bronze_root=tmp_path / "bronze"
    )

    correction_connection, _ = _watermark_connection(date(2026, 9, 18))
    correction_client = Mock(spec=TwelveDataClient)
    correction_client.fetch_daily_time_series_since.return_value = _payload(
        "AAPL",
        [
            _row("2026-09-18", close="252"),
            _row("2026-09-19", close="253"),
        ],
    )
    run_b = MarketIngestionService(correction_client).ingest_symbol_incrementally_to_bronze(
        correction_connection, "AAPL", run_at=_RUN_AT, bronze_root=tmp_path / "bronze"
    )

    assert run_a.location.metadata.run_id == reused_run_a.location.metadata.run_id
    assert run_a.location.metadata.run_id != run_b.location.metadata.run_id
    assert bootstrap_client.fetch_daily_time_series.call_count == 1
    correction_client.fetch_daily_time_series_since.assert_called_once_with(
        "AAPL", start_date=date(2026, 9, 15)
    )
    assert read_parquet(run_b.location).column("close").to_pylist() == [
        Decimal("252"),
        Decimal("253"),
    ]


def test_incremental_market_zero_row_weekend_response_remains_valid(
    tmp_path: Path,
) -> None:
    connection, _ = _watermark_connection(date(2026, 9, 18))
    client = Mock(spec=TwelveDataClient)
    client.fetch_daily_time_series_since.return_value = _payload("AAPL")

    result = MarketIngestionService(client).ingest_symbol_incrementally_to_bronze(
        connection,
        "AAPL",
        run_at=_RUN_AT,
        bronze_root=tmp_path / "bronze",
    )

    assert result.record_count == 0
    assert read_parquet(result.location).num_rows == 0
    client.fetch_daily_time_series_since.assert_called_once_with(
        "AAPL",
        start_date=date(2026, 9, 15),
    )
