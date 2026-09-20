from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock

import pyarrow as pa
import pytest

import finstream.market.ingestion as market_ingestion
from finstream.bronze.json_storage import (
    BronzeRawJsonValidationError,
    read_raw_json,
    write_raw_json,
)
from finstream.bronze.models import BronzeRunLocation
from finstream.bronze.recovery import BronzeRecoveryError
from finstream.bronze.parquet_storage import (
    BronzeParquetWriteError,
    read_parquet,
    write_parquet,
)
from finstream.market.bronze import (
    DAILY_MARKET_PRICE_SCHEMA,
    MARKET_BRONZE_DATASET,
    MARKET_BRONZE_SOURCE,
    MarketBronzeValidationError,
    daily_market_prices_to_table,
)
from finstream.market.ingestion import MarketIngestionService
from finstream.market.models import DailyMarketPrice
from finstream.market.parsing import MarketDataValidationError, parse_daily_time_series
from finstream.market.twelve_data import TwelveDataClient, TwelveDataError


_RUN_AT = datetime(2026, 8, 30, 12, 0, 0, 123456, tzinfo=timezone.utc)


def _location(tmp_path: Path) -> BronzeRunLocation:
    return BronzeRunLocation.from_run(
        root=tmp_path / "bronze",
        source=MARKET_BRONZE_SOURCE,
        dataset=MARKET_BRONZE_DATASET,
        ingested_at=_RUN_AT,
        entity="AAPL",
    )


def _payload(symbol: str, values: list[dict[str, str]] | None = None) -> dict:
    return {
        "meta": {"symbol": symbol, "interval": "1day"},
        "values": [] if values is None else values,
    }


def _row(trading_date: str = "2026-08-28") -> dict[str, str]:
    return {
        "datetime": trading_date,
        "open": "150.100000000000000001",
        "high": "155.500000000000000001",
        "low": "149.250000000000000001",
        "close": "153.330000000000000001",
        "volume": "1234567",
    }


def _record(
    *,
    symbol: str = "AAPL",
    trading_date: date = date(2026, 8, 28),
    open_price: Decimal = Decimal("150.100000000000000001"),
) -> DailyMarketPrice:
    return DailyMarketPrice(
        symbol=symbol,
        trading_date=trading_date,
        open=open_price,
        high=Decimal("155.500000000000000001"),
        low=Decimal("149.250000000000000001"),
        close=Decimal("153.330000000000000001"),
        volume=1234567,
    )


def test_daily_market_price_schema_is_source_aligned_and_stable() -> None:
    assert DAILY_MARKET_PRICE_SCHEMA.names == [
        "symbol",
        "trading_date",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]
    assert DAILY_MARKET_PRICE_SCHEMA.field("symbol") == pa.field(
        "symbol",
        pa.string(),
        nullable=False,
    )
    assert DAILY_MARKET_PRICE_SCHEMA.field("trading_date") == pa.field(
        "trading_date",
        pa.date32(),
        nullable=False,
    )
    for field_name in ("open", "high", "low", "close"):
        assert DAILY_MARKET_PRICE_SCHEMA.field(field_name) == pa.field(
            field_name,
            pa.decimal128(38, 18),
            nullable=False,
        )
    assert DAILY_MARKET_PRICE_SCHEMA.field("volume") == pa.field(
        "volume",
        pa.int64(),
        nullable=True,
    )


def test_daily_market_price_conversion_preserves_order_and_decimals() -> None:
    records = [
        _record(trading_date=date(2026, 8, 28)),
        _record(trading_date=date(2026, 8, 27)),
    ]

    table = daily_market_prices_to_table(records)

    assert table.schema.equals(DAILY_MARKET_PRICE_SCHEMA)
    assert table.column("trading_date").to_pylist() == [
        date(2026, 8, 28),
        date(2026, 8, 27),
    ]
    assert table.column("open").to_pylist() == [
        Decimal("150.100000000000000001"),
        Decimal("150.100000000000000001"),
    ]


def test_daily_market_price_conversion_supports_zero_records() -> None:
    table = daily_market_prices_to_table([])

    assert table.num_rows == 0
    assert table.schema.equals(DAILY_MARKET_PRICE_SCHEMA)


@pytest.mark.parametrize(
    "records",
    [
        pytest.param([object()], id="non-market-record"),
        pytest.param(
            [_record(open_price=Decimal("1e100"))],
            id="unrepresentable-decimal",
        ),
    ],
)
def test_daily_market_price_conversion_rejects_invalid_input(
    records: list[object],
) -> None:
    with pytest.raises(MarketBronzeValidationError):
        daily_market_prices_to_table(records)  # type: ignore[arg-type]


def test_ingest_symbol_to_bronze_writes_one_raw_and_one_structured_artifact(
    tmp_path: Path,
) -> None:
    payload = _payload("AAPL", [_row()])
    client = Mock(spec=TwelveDataClient)
    client.fetch_daily_time_series.return_value = payload
    service = MarketIngestionService(client)

    result = service.ingest_symbol_to_bronze(
        " aapl ",
        run_at=_RUN_AT,
        bronze_root=tmp_path / "bronze",
        start_date=date(2026, 8, 17),
        end_date=date(2026, 8, 21),
    )

    client.fetch_daily_time_series.assert_called_once_with(
        "AAPL",
        start_date=date(2026, 8, 17),
        end_date=date(2026, 8, 21),
    )
    assert result.symbol == "AAPL"
    assert result.location.metadata.source == MARKET_BRONZE_SOURCE
    assert result.location.metadata.dataset == MARKET_BRONZE_DATASET
    assert result.location.metadata.ingested_at == _RUN_AT
    assert result.record_count == 1
    assert read_raw_json(result.location) == payload
    assert read_parquet(result.location).equals(
        daily_market_prices_to_table(parse_daily_time_series(payload, expected_symbol="AAPL"))
    )
    assert sorted(path.name for path in result.location.directory.iterdir()) == [
        "data.parquet",
        "payload.json",
    ]
    assert result.raw_json_path.is_file()
    assert result.parquet_path.is_file()


def test_same_timestamp_distinguishes_symbols_and_reuses_the_same_symbol_run(
    tmp_path: Path,
) -> None:
    client = Mock(spec=TwelveDataClient)
    client.fetch_daily_time_series.side_effect = [
        _payload("AAPL", [_row()]),
        _payload("MSFT", [_row()]),
    ]
    service = MarketIngestionService(client)

    aapl = service.ingest_symbol_to_bronze(" aapl ", run_at=_RUN_AT, bronze_root=tmp_path / "bronze")
    msft = service.ingest_symbol_to_bronze("MSFT", run_at=_RUN_AT, bronze_root=tmp_path / "bronze")
    replayed_aapl = service.ingest_symbol_to_bronze("AAPL", run_at=_RUN_AT, bronze_root=tmp_path / "bronze")

    assert aapl.location.metadata.ingested_at == msft.location.metadata.ingested_at == _RUN_AT
    assert aapl.location.metadata.run_id != msft.location.metadata.run_id
    assert aapl.location.directory != msft.location.directory
    assert replayed_aapl.location == aapl.location
    assert client.fetch_daily_time_series.call_count == 2


def test_no_data_market_bronze_run_preserves_raw_payload_and_full_schema(
    tmp_path: Path,
) -> None:
    payload = _payload("AAPL")
    client = Mock(spec=TwelveDataClient)
    client.fetch_daily_time_series.return_value = payload

    result = MarketIngestionService(client).ingest_symbol_to_bronze(
        "AAPL",
        run_at=_RUN_AT,
        bronze_root=tmp_path / "bronze",
    )

    table = read_parquet(result.location)
    assert result.record_count == 0
    assert read_raw_json(result.location) == payload
    assert table.num_rows == 0
    assert table.schema.equals(DAILY_MARKET_PRICE_SCHEMA)


def test_stored_raw_payload_reprocesses_without_another_provider_request(
    tmp_path: Path,
) -> None:
    payload = _payload("AAPL", [_row()])
    client = Mock(spec=TwelveDataClient)
    client.fetch_daily_time_series.return_value = payload
    service = MarketIngestionService(client)

    result = service.ingest_symbol_to_bronze(
        "AAPL",
        run_at=_RUN_AT,
        bronze_root=tmp_path / "bronze",
    )

    assert parse_daily_time_series(
        read_raw_json(result.location),
        expected_symbol="AAPL",
    ) == parse_daily_time_series(payload, expected_symbol="AAPL")
    client.fetch_daily_time_series.assert_called_once()


def test_same_run_raw_only_reconstructs_then_reuses_without_provider_request(
    tmp_path: Path,
) -> None:
    location = _location(tmp_path)
    payload = _payload("AAPL", [_row()])
    write_raw_json(location, payload)
    client = Mock(spec=TwelveDataClient)
    service = MarketIngestionService(client)

    reconstructed = service.ingest_symbol_to_bronze(
        "AAPL", run_at=_RUN_AT, bronze_root=tmp_path / "bronze"
    )
    raw_bytes = reconstructed.raw_json_path.read_bytes()
    reused = service.ingest_symbol_to_bronze(
        "AAPL", run_at=_RUN_AT, bronze_root=tmp_path / "bronze"
    )

    assert reconstructed.record_count == reused.record_count == 1
    assert reconstructed.parquet_path.is_file()
    assert reused.raw_json_path.read_bytes() == raw_bytes
    client.fetch_daily_time_series.assert_not_called()


def test_parquet_only_run_requires_operator_action(tmp_path: Path) -> None:
    location = _location(tmp_path)
    write_parquet(
        location,
        daily_market_prices_to_table(parse_daily_time_series(_payload("AAPL", [_row()]), expected_symbol="AAPL")),
    )
    client = Mock(spec=TwelveDataClient)

    with pytest.raises(BronzeRecoveryError, match="without Raw JSON"):
        MarketIngestionService(client).ingest_symbol_to_bronze(
            "AAPL", run_at=_RUN_AT, bronze_root=tmp_path / "bronze"
        )

    client.fetch_daily_time_series.assert_not_called()


def test_mismatched_valid_raw_and_parquet_require_operator_action(
    tmp_path: Path,
) -> None:
    location = _location(tmp_path)
    raw_payload = _payload("AAPL", [_row("2026-08-28")])
    parquet_payload = _payload("AAPL", [_row("2026-08-27")])
    write_raw_json(location, raw_payload)
    write_parquet(
        location,
        daily_market_prices_to_table(
            parse_daily_time_series(parquet_payload, expected_symbol="AAPL")
        ),
    )
    original_parquet = location.directory.joinpath("data.parquet").read_bytes()
    client = Mock(spec=TwelveDataClient)

    with pytest.raises(BronzeRecoveryError, match="does not match"):
        MarketIngestionService(client).ingest_symbol_to_bronze(
            "AAPL", run_at=_RUN_AT, bronze_root=tmp_path / "bronze"
        )

    assert location.directory.joinpath("data.parquet").read_bytes() == original_parquet
    client.fetch_daily_time_series.assert_not_called()


def test_unexpected_run_artifact_requires_operator_action(tmp_path: Path) -> None:
    location = _location(tmp_path)
    payload = _payload("AAPL", [_row()])
    write_raw_json(location, payload)
    write_parquet(
        location,
        daily_market_prices_to_table(
            parse_daily_time_series(payload, expected_symbol="AAPL")
        ),
    )
    extra_path = location.directory / "unexpected.tmp"
    extra_path.write_text("operator-owned", encoding="utf-8")
    client = Mock(spec=TwelveDataClient)

    with pytest.raises(BronzeRecoveryError, match="unexpected artifacts"):
        MarketIngestionService(client).ingest_symbol_to_bronze(
            "AAPL", run_at=_RUN_AT, bronze_root=tmp_path / "bronze"
        )

    assert extra_path.read_text(encoding="utf-8") == "operator-owned"
    client.fetch_daily_time_series.assert_not_called()


def test_bronze_prevalidation_fails_before_provider_request(
    tmp_path: Path,
) -> None:
    client = Mock(spec=TwelveDataClient)
    service = MarketIngestionService(client)

    with pytest.raises(ValueError):
        service.ingest_symbol_to_bronze(
            "AAPL",
            run_at=datetime(2026, 8, 30, 12, 0, 0),
            bronze_root=tmp_path / "bronze",
        )
    with pytest.raises(ValueError, match="Symbol must not be blank"):
        service.ingest_symbol_to_bronze(
            " ",
            run_at=_RUN_AT,
            bronze_root=tmp_path / "bronze",
        )

    client.fetch_daily_time_series.assert_not_called()


def test_existing_bronze_artifact_prevents_provider_request(tmp_path: Path) -> None:
    location = _location(tmp_path)
    write_raw_json(location, {"existing": "artifact"})
    client = Mock(spec=TwelveDataClient)
    service = MarketIngestionService(client)

    with pytest.raises(BronzeRecoveryError):
        service.ingest_symbol_to_bronze(
            "AAPL",
            run_at=_RUN_AT,
            bronze_root=tmp_path / "bronze",
        )

    assert read_raw_json(location) == {"existing": "artifact"}
    client.fetch_daily_time_series.assert_not_called()


@pytest.mark.parametrize(
    "failure",
    [
        pytest.param(TwelveDataError("provider failed"), id="provider"),
        pytest.param(_payload("MSFT", [_row()]), id="parser"),
    ],
)
def test_provider_and_parser_failures_leave_no_bronze_artifacts(
    tmp_path: Path,
    failure: object,
) -> None:
    client = Mock(spec=TwelveDataClient)
    if isinstance(failure, Exception):
        client.fetch_daily_time_series.side_effect = failure
    else:
        client.fetch_daily_time_series.return_value = failure
    service = MarketIngestionService(client)

    with pytest.raises((TwelveDataError, MarketDataValidationError)):
        service.ingest_symbol_to_bronze(
            "AAPL",
            run_at=_RUN_AT,
            bronze_root=tmp_path / "bronze",
        )

    assert not (tmp_path / "bronze").exists()


def test_raw_json_failure_does_not_attempt_parquet_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = _payload("AAPL", [_row()])
    client = Mock(spec=TwelveDataClient)
    client.fetch_daily_time_series.return_value = payload
    parquet_writer = Mock()
    monkeypatch.setattr(
        market_ingestion,
        "write_raw_json",
        Mock(side_effect=BronzeRawJsonValidationError("raw failure")),
    )
    monkeypatch.setattr(market_ingestion, "write_parquet", parquet_writer)

    with pytest.raises(BronzeRawJsonValidationError, match="raw failure"):
        MarketIngestionService(client).ingest_symbol_to_bronze(
            "AAPL",
            run_at=_RUN_AT,
            bronze_root=tmp_path / "bronze",
        )

    parquet_writer.assert_not_called()


def test_parquet_failure_retains_valid_raw_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = _payload("AAPL", [_row()])
    client = Mock(spec=TwelveDataClient)
    client.fetch_daily_time_series.return_value = payload
    monkeypatch.setattr(
        market_ingestion,
        "write_parquet",
        Mock(side_effect=BronzeParquetWriteError("parquet failure")),
    )

    with pytest.raises(BronzeParquetWriteError, match="parquet failure"):
        MarketIngestionService(client).ingest_symbol_to_bronze(
            "AAPL",
            run_at=_RUN_AT,
            bronze_root=tmp_path / "bronze",
        )

    assert read_raw_json(_location(tmp_path)) == payload

