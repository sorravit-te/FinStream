"""Validation and parsing for Twelve Data daily market payloads."""

import re
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

from finstream.market.models import DailyMarketPrice


_DAILY_INTERVAL = "1day"
_DAILY_DATE_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}\Z")
_PRICE_FIELDS = ("open", "high", "low", "close")


class MarketDataValidationError(ValueError):
    """Raised when a successful market payload contains invalid data."""


def _normalize_symbol(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise MarketDataValidationError(f"{field_name} must not be blank")
    return value.strip().upper()


def _parse_trading_date(value: object) -> date:
    if not isinstance(value, str) or not _DAILY_DATE_PATTERN.fullmatch(value):
        raise MarketDataValidationError("datetime must use YYYY-MM-DD format")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise MarketDataValidationError("datetime is not a valid date") from exc


def _parse_price(row: dict[str, Any], field_name: str) -> Decimal:
    value = row.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise MarketDataValidationError(
            f"{field_name} must be a non-blank decimal string"
        )
    try:
        parsed_value = Decimal(value.strip())
    except InvalidOperation as exc:
        raise MarketDataValidationError(f"{field_name} is not a valid decimal") from exc
    if not parsed_value.is_finite():
        raise MarketDataValidationError(f"{field_name} must be finite")
    return parsed_value


def _parse_volume(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise MarketDataValidationError("volume must be an integer")
    if isinstance(value, int):
        parsed_value = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed_value = int(value.strip())
        except ValueError as exc:
            raise MarketDataValidationError("volume must be an integer") from exc
    else:
        raise MarketDataValidationError("volume must be an integer")
    if parsed_value < 0:
        raise MarketDataValidationError("volume must not be negative")
    return parsed_value


def _validate_ohlc(
    open_price: Decimal,
    high_price: Decimal,
    low_price: Decimal,
    close_price: Decimal,
) -> None:
    if high_price < low_price:
        raise MarketDataValidationError("high must be greater than or equal to low")
    if high_price < open_price:
        raise MarketDataValidationError("high must be greater than or equal to open")
    if high_price < close_price:
        raise MarketDataValidationError("high must be greater than or equal to close")
    if low_price > open_price:
        raise MarketDataValidationError("low must be less than or equal to open")
    if low_price > close_price:
        raise MarketDataValidationError("low must be less than or equal to close")


def parse_daily_time_series(
    payload: dict[str, Any],
    *,
    expected_symbol: str,
) -> list[DailyMarketPrice]:
    """Parse a successful Twelve Data daily time-series payload."""
    normalized_expected_symbol = _normalize_symbol(
        expected_symbol,
        field_name="expected_symbol",
    )
    if not isinstance(payload, dict):
        raise MarketDataValidationError("payload must be an object")

    meta = payload.get("meta")
    if not isinstance(meta, dict):
        raise MarketDataValidationError("meta must be an object")
    provider_symbol = _normalize_symbol(
        meta.get("symbol"),
        field_name="meta.symbol",
    )
    if provider_symbol != normalized_expected_symbol:
        raise MarketDataValidationError("provider symbol does not match expected symbol")
    if meta.get("interval") != _DAILY_INTERVAL:
        raise MarketDataValidationError("meta.interval must be 1day")

    if "values" not in payload or not isinstance(payload["values"], list):
        raise MarketDataValidationError("values must be a list")

    records: list[DailyMarketPrice] = []
    seen_dates: set[date] = set()
    for value in payload["values"]:
        if not isinstance(value, dict):
            raise MarketDataValidationError("each values item must be an object")

        trading_date = _parse_trading_date(value.get("datetime"))
        if trading_date in seen_dates:
            raise MarketDataValidationError("duplicate trading date in payload")

        prices = {field: _parse_price(value, field) for field in _PRICE_FIELDS}
        _validate_ohlc(
            prices["open"],
            prices["high"],
            prices["low"],
            prices["close"],
        )
        records.append(
            DailyMarketPrice(
                symbol=provider_symbol,
                trading_date=trading_date,
                open=prices["open"],
                high=prices["high"],
                low=prices["low"],
                close=prices["close"],
                volume=_parse_volume(value.get("volume")),
            )
        )
        seen_dates.add(trading_date)

    return records
