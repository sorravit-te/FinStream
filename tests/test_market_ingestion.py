from datetime import date
from decimal import Decimal
from unittest.mock import Mock, call

import pytest

from finflow.market.ingestion import MarketIngestionService
from finflow.market.models import DailyMarketPrice
from finflow.market.parsing import MarketDataValidationError
from finflow.market.persistence import MarketPersistenceResult, MarketRecordStore
from finflow.market.twelve_data import TwelveDataClient, TwelveDataError


def _payload(symbol: str, values: list[dict[str, str]] | None = None) -> dict:
    return {
        "meta": {"symbol": symbol, "interval": "1day"},
        "values": []
        if values is None
        else values,
    }


def _row(trading_date: str = "2026-01-05") -> dict[str, str]:
    return {
        "datetime": trading_date,
        "open": "150.10",
        "high": "155.50",
        "low": "149.25",
        "close": "153.33",
        "volume": "1234567",
    }


def test_ingests_normalized_single_symbol_and_forwards_dates() -> None:
    client = Mock(spec=TwelveDataClient)
    client.fetch_daily_time_series.return_value = _payload("AAPL", [_row()])
    service = MarketIngestionService(client)
    start_date = date(2026, 1, 1)
    end_date = date(2026, 1, 31)

    records = service.ingest_symbol(
        " aapl ",
        start_date=start_date,
        end_date=end_date,
    )

    client.fetch_daily_time_series.assert_called_once_with(
        "AAPL",
        start_date=start_date,
        end_date=end_date,
    )
    assert records == [
        DailyMarketPrice(
            symbol="AAPL",
            trading_date=date(2026, 1, 5),
            open=Decimal("150.10"),
            high=Decimal("155.50"),
            low=Decimal("149.25"),
            close=Decimal("153.33"),
            volume=1234567,
        )
    ]


def test_valid_no_data_payload_returns_empty_list() -> None:
    client = Mock(spec=TwelveDataClient)
    client.fetch_daily_time_series.return_value = _payload("AAPL")
    service = MarketIngestionService(client)

    assert service.ingest_symbol("AAPL") == []


def test_provider_failure_propagates() -> None:
    client = Mock(spec=TwelveDataClient)
    client.fetch_daily_time_series.side_effect = TwelveDataError("provider failed")
    service = MarketIngestionService(client)

    with pytest.raises(TwelveDataError, match="provider failed"):
        service.ingest_symbol("AAPL")


def test_payload_validation_failure_propagates() -> None:
    client = Mock(spec=TwelveDataClient)
    client.fetch_daily_time_series.return_value = _payload("MSFT")
    service = MarketIngestionService(client)

    with pytest.raises(MarketDataValidationError, match="does not match"):
        service.ingest_symbol("AAPL")


def test_ingests_multiple_symbols_sequentially_in_input_order() -> None:
    client = Mock(spec=TwelveDataClient)
    client.fetch_daily_time_series.side_effect = [
        _payload("AAPL", [_row("2026-01-05")]),
        _payload("MSFT", [_row("2026-01-06")]),
    ]
    service = MarketIngestionService(client)

    results = service.ingest_symbols([" aapl ", "msft"])

    assert list(results) == ["AAPL", "MSFT"]
    assert results["AAPL"][0].symbol == "AAPL"
    assert results["MSFT"][0].symbol == "MSFT"
    assert client.fetch_daily_time_series.call_args_list == [
        call("AAPL", start_date=None, end_date=None),
        call("MSFT", start_date=None, end_date=None),
    ]


def test_duplicate_symbols_are_rejected_before_provider_calls() -> None:
    client = Mock(spec=TwelveDataClient)
    service = MarketIngestionService(client)

    with pytest.raises(ValueError, match="Duplicate symbol"):
        service.ingest_symbols(["AAPL", " aapl "])

    client.fetch_daily_time_series.assert_not_called()


def test_blank_symbol_is_rejected_before_any_provider_call() -> None:
    client = Mock(spec=TwelveDataClient)
    service = MarketIngestionService(client)

    with pytest.raises(ValueError, match="Symbol must not be blank"):
        service.ingest_symbols(["AAPL", "   ", "MSFT"])

    client.fetch_daily_time_series.assert_not_called()


def test_multi_symbol_failure_stops_later_processing() -> None:
    client = Mock(spec=TwelveDataClient)
    client.fetch_daily_time_series.side_effect = [
        _payload("AAPL"),
        TwelveDataError("provider failed"),
        _payload("NVDA"),
    ]
    service = MarketIngestionService(client)

    with pytest.raises(TwelveDataError, match="provider failed"):
        service.ingest_symbols(["AAPL", "MSFT", "NVDA"])

    assert client.fetch_daily_time_series.call_args_list == [
        call("AAPL", start_date=None, end_date=None),
        call("MSFT", start_date=None, end_date=None),
    ]


def test_ingests_and_persists_single_symbol() -> None:
    client = Mock(spec=TwelveDataClient)
    client.fetch_daily_time_series.return_value = _payload("AAPL", [_row()])
    store = Mock(spec=MarketRecordStore)
    expected_result = MarketPersistenceResult(1, 0, 0, 1)
    store.upsert.return_value = expected_result
    service = MarketIngestionService(client, store)

    result = service.ingest_and_persist_symbol(" aapl ")

    assert result is expected_result
    persisted_records = store.upsert.call_args.args[0]
    assert [record.symbol for record in persisted_records] == ["AAPL"]


def test_persist_without_store_fails_before_provider_request() -> None:
    client = Mock(spec=TwelveDataClient)
    service = MarketIngestionService(client)

    with pytest.raises(RuntimeError, match="store is not configured"):
        service.ingest_and_persist_symbol("AAPL")

    client.fetch_daily_time_series.assert_not_called()


def test_multi_symbol_failure_does_not_persist_partial_results() -> None:
    client = Mock(spec=TwelveDataClient)
    client.fetch_daily_time_series.side_effect = [
        _payload("AAPL", [_row()]),
        TwelveDataError("provider failed"),
    ]
    store = Mock(spec=MarketRecordStore)
    service = MarketIngestionService(client, store)

    with pytest.raises(TwelveDataError, match="provider failed"):
        service.ingest_and_persist_symbols(["AAPL", "MSFT"])

    store.upsert.assert_not_called()


def test_successful_multi_symbol_ingestion_uses_one_persistence_call() -> None:
    client = Mock(spec=TwelveDataClient)
    client.fetch_daily_time_series.side_effect = [
        _payload("AAPL", [_row("2026-01-05")]),
        _payload("MSFT", [_row("2026-01-06")]),
    ]
    store = Mock(spec=MarketRecordStore)
    expected_result = MarketPersistenceResult(2, 0, 0, 2)
    store.upsert.return_value = expected_result
    service = MarketIngestionService(client, store)

    result = service.ingest_and_persist_symbols(["AAPL", "MSFT"])

    assert result is expected_result
    store.upsert.assert_called_once()
    persisted_records = store.upsert.call_args.args[0]
    assert [record.symbol for record in persisted_records] == ["AAPL", "MSFT"]
