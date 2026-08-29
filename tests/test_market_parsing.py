from datetime import date
from decimal import Decimal
from typing import Any

import pytest

from finstream.market.models import DailyMarketPrice
from finstream.market.parsing import MarketDataValidationError, parse_daily_time_series


def _row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "datetime": "2026-01-05",
        "open": "150.1200",
        "high": "155.5000",
        "low": "149.2500",
        "close": "153.3300",
        "volume": "1234567",
    }
    row.update(overrides)
    return row


def _payload(values: list[object] | None = None) -> dict[str, Any]:
    return {
        "meta": {"symbol": "AAPL", "interval": "1day"},
        "values": [_row()] if values is None else values,
    }


def test_parses_valid_daily_ohlcv_row_and_normalizes_symbol() -> None:
    records = parse_daily_time_series(_payload(), expected_symbol=" aapl ")

    assert records == [
        DailyMarketPrice(
            symbol="AAPL",
            trading_date=date(2026, 1, 5),
            open=Decimal("150.1200"),
            high=Decimal("155.5000"),
            low=Decimal("149.2500"),
            close=Decimal("153.3300"),
            volume=1234567,
        )
    ]
    assert records[0].open.as_tuple() == Decimal("150.1200").as_tuple()


def test_empty_values_is_valid() -> None:
    assert parse_daily_time_series(_payload([]), expected_symbol="AAPL") == []


@pytest.mark.parametrize("values", [None, "not-a-list", {}])
def test_rejects_missing_or_non_list_values(values: object) -> None:
    payload = _payload()
    if values is None:
        del payload["values"]
    else:
        payload["values"] = values

    with pytest.raises(MarketDataValidationError, match="values must be a list"):
        parse_daily_time_series(payload, expected_symbol="AAPL")


@pytest.mark.parametrize("meta", [None, [], "invalid"])
def test_rejects_missing_or_invalid_meta(meta: object) -> None:
    payload = _payload()
    if meta is None:
        del payload["meta"]
    else:
        payload["meta"] = meta

    with pytest.raises(MarketDataValidationError, match="meta must be an object"):
        parse_daily_time_series(payload, expected_symbol="AAPL")


@pytest.mark.parametrize("provider_symbol", [None, "", "   "])
def test_rejects_missing_or_blank_provider_symbol(provider_symbol: object) -> None:
    payload = _payload()
    payload["meta"]["symbol"] = provider_symbol

    with pytest.raises(MarketDataValidationError, match="meta.symbol"):
        parse_daily_time_series(payload, expected_symbol="AAPL")


def test_rejects_blank_expected_symbol() -> None:
    with pytest.raises(MarketDataValidationError, match="expected_symbol"):
        parse_daily_time_series(_payload(), expected_symbol="   ")


def test_rejects_provider_symbol_mismatch() -> None:
    with pytest.raises(MarketDataValidationError, match="does not match"):
        parse_daily_time_series(_payload(), expected_symbol="MSFT")


@pytest.mark.parametrize("interval", [None, "", "5min", " 1day "])
def test_rejects_missing_or_unexpected_interval(interval: object) -> None:
    payload = _payload()
    payload["meta"]["interval"] = interval

    with pytest.raises(MarketDataValidationError, match="meta.interval"):
        parse_daily_time_series(payload, expected_symbol="AAPL")


@pytest.mark.parametrize(
    "trading_date",
    ["2026-01-05 09:30:00", "2026-1-5", "2026-02-30", None],
)
def test_rejects_invalid_daily_date(trading_date: object) -> None:
    with pytest.raises(MarketDataValidationError, match="datetime"):
        parse_daily_time_series(
            _payload([_row(datetime=trading_date)]),
            expected_symbol="AAPL",
        )


@pytest.mark.parametrize("field", ["open", "high", "low", "close"])
def test_rejects_missing_ohlc_field(field: str) -> None:
    row = _row()
    del row[field]

    with pytest.raises(MarketDataValidationError, match=field):
        parse_daily_time_series(_payload([row]), expected_symbol="AAPL")


@pytest.mark.parametrize("invalid_value", ["", "   ", "not-a-decimal", 100])
def test_rejects_invalid_ohlc_value(invalid_value: object) -> None:
    with pytest.raises(MarketDataValidationError, match="open"):
        parse_daily_time_series(
            _payload([_row(open=invalid_value)]),
            expected_symbol="AAPL",
        )


@pytest.mark.parametrize("non_finite", ["NaN", "Infinity", "-Infinity"])
def test_rejects_non_finite_price(non_finite: str) -> None:
    with pytest.raises(MarketDataValidationError, match="finite"):
        parse_daily_time_series(
            _payload([_row(open=non_finite)]),
            expected_symbol="AAPL",
        )


@pytest.mark.parametrize(
    "prices",
    [
        {"high": "148", "low": "149"},
        {"open": "156", "high": "155"},
        {"close": "156", "high": "155"},
        {"open": "148", "low": "149"},
        {"close": "148", "low": "149"},
    ],
)
def test_rejects_ohlc_relationship_violation(prices: dict[str, str]) -> None:
    with pytest.raises(MarketDataValidationError):
        parse_daily_time_series(
            _payload([_row(**prices)]),
            expected_symbol="AAPL",
        )


def test_allows_missing_volume() -> None:
    row = _row()
    del row["volume"]

    records = parse_daily_time_series(_payload([row]), expected_symbol="AAPL")

    assert records[0].volume is None


@pytest.mark.parametrize("volume", [0, "0"])
def test_allows_zero_volume(volume: object) -> None:
    records = parse_daily_time_series(
        _payload([_row(volume=volume)]),
        expected_symbol="AAPL",
    )

    assert records[0].volume == 0


@pytest.mark.parametrize("volume", ["", "invalid", "1.5", -1, "-1", 1.5])
def test_rejects_invalid_volume(volume: object) -> None:
    with pytest.raises(MarketDataValidationError, match="volume"):
        parse_daily_time_series(
            _payload([_row(volume=volume)]),
            expected_symbol="AAPL",
        )


def test_rejects_non_object_value_item() -> None:
    with pytest.raises(MarketDataValidationError, match="values item"):
        parse_daily_time_series(_payload(["invalid"]), expected_symbol="AAPL")


def test_rejects_duplicate_trading_date() -> None:
    with pytest.raises(MarketDataValidationError, match="duplicate trading date"):
        parse_daily_time_series(
            _payload([_row(), _row(close="152.00")]),
            expected_symbol="AAPL",
        )


def test_preserves_provider_ordering() -> None:
    payload = _payload(
        [
            _row(datetime="2026-01-06"),
            _row(datetime="2026-01-05"),
        ]
    )

    records = parse_daily_time_series(payload, expected_symbol="AAPL")

    assert [record.trading_date for record in records] == [
        date(2026, 1, 6),
        date(2026, 1, 5),
    ]
