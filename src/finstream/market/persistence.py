"""Duplicate-safe local persistence for source-aligned market records."""

import json
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Protocol

from finstream.market.models import DailyMarketPrice


_RECORD_FIELDS = {
    "symbol",
    "trading_date",
    "open",
    "high",
    "low",
    "close",
    "volume",
}


@dataclass(frozen=True)
class MarketPersistenceResult:
    """Summary of one market-record persistence operation."""

    inserted: int
    updated: int
    unchanged: int
    total: int


class MarketPersistenceError(Exception):
    """Raised when market records cannot be read, validated, or written."""


class MarketRecordStore(Protocol):
    """Persistence boundary for source-aligned market records."""

    def upsert(
        self,
        records: Iterable[DailyMarketPrice],
    ) -> MarketPersistenceResult:
        """Insert or update records by their natural key."""
        ...


def _serialize_record(record: DailyMarketPrice) -> dict[str, str | int | None]:
    return {
        "symbol": record.symbol,
        "trading_date": record.trading_date.isoformat(),
        "open": str(record.open),
        "high": str(record.high),
        "low": str(record.low),
        "close": str(record.close),
        "volume": record.volume,
    }


def _deserialize_record(value: object, *, location: str) -> DailyMarketPrice:
    if not isinstance(value, dict) or set(value) != _RECORD_FIELDS:
        raise MarketPersistenceError(f"{location} has an invalid record shape")

    symbol = value["symbol"]
    if (
        not isinstance(symbol, str)
        or not symbol.strip()
        or symbol != symbol.strip().upper()
    ):
        raise MarketPersistenceError(f"{location} has an invalid symbol")

    trading_date_value = value["trading_date"]
    if not isinstance(trading_date_value, str):
        raise MarketPersistenceError(f"{location} has an invalid trading date")
    try:
        trading_date = date.fromisoformat(trading_date_value)
    except ValueError as exc:
        raise MarketPersistenceError(
            f"{location} has an invalid trading date"
        ) from exc
    if trading_date.isoformat() != trading_date_value:
        raise MarketPersistenceError(f"{location} has an invalid trading date")

    prices: dict[str, Decimal] = {}
    for field_name in ("open", "high", "low", "close"):
        field_value = value[field_name]
        if not isinstance(field_value, str) or not field_value.strip():
            raise MarketPersistenceError(f"{location} has an invalid {field_name}")
        try:
            price = Decimal(field_value)
        except InvalidOperation as exc:
            raise MarketPersistenceError(
                f"{location} has an invalid {field_name}"
            ) from exc
        if not price.is_finite():
            raise MarketPersistenceError(f"{location} has an invalid {field_name}")
        prices[field_name] = price

    if (
        prices["high"] < prices["low"]
        or prices["high"] < prices["open"]
        or prices["high"] < prices["close"]
        or prices["low"] > prices["open"]
        or prices["low"] > prices["close"]
    ):
        raise MarketPersistenceError(f"{location} has invalid OHLC relationships")

    volume = value["volume"]
    if volume is not None and (
        isinstance(volume, bool) or not isinstance(volume, int) or volume < 0
    ):
        raise MarketPersistenceError(f"{location} has an invalid volume")

    return DailyMarketPrice(
        symbol=symbol,
        trading_date=trading_date,
        open=prices["open"],
        high=prices["high"],
        low=prices["low"],
        close=prices["close"],
        volume=volume,
    )


class JsonMarketRecordStore:
    """Store the current source-aligned market snapshot as a JSON array."""

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)

    def upsert(
        self,
        records: Iterable[DailyMarketPrice],
    ) -> MarketPersistenceResult:
        """Insert or update records atomically by symbol and trading date."""
        existing_records = self._read_existing_records()
        incoming_records: dict[tuple[str, date], DailyMarketPrice] = {}
        for index, record in enumerate(records):
            if not isinstance(record, DailyMarketPrice):
                raise MarketPersistenceError(
                    f"incoming record {index} is not a DailyMarketPrice"
                )
            validated_record = _deserialize_record(
                _serialize_record(record),
                location=f"incoming record {index}",
            )
            key = (validated_record.symbol, validated_record.trading_date)
            if key in incoming_records:
                raise MarketPersistenceError("duplicate natural key in incoming records")
            incoming_records[key] = validated_record

        inserted = 0
        updated = 0
        unchanged = 0
        merged_records = dict(existing_records)
        for key, incoming_record in incoming_records.items():
            existing_record = existing_records.get(key)
            if existing_record is None:
                inserted += 1
                merged_records[key] = incoming_record
            elif _serialize_record(existing_record) == _serialize_record(incoming_record):
                unchanged += 1
            else:
                updated += 1
                merged_records[key] = incoming_record

        if inserted or updated:
            self._write_records(merged_records.values())

        return MarketPersistenceResult(
            inserted=inserted,
            updated=updated,
            unchanged=unchanged,
            total=len(merged_records),
        )

    def _read_existing_records(
        self,
    ) -> dict[tuple[str, date], DailyMarketPrice]:
        if not self._path.exists():
            return {}
        try:
            with self._path.open(encoding="utf-8") as file:
                payload = json.load(file)
        except (OSError, json.JSONDecodeError) as exc:
            raise MarketPersistenceError("Could not read market record file") from exc

        if not isinstance(payload, list):
            raise MarketPersistenceError("Market record file must contain a JSON array")

        records: dict[tuple[str, date], DailyMarketPrice] = {}
        for index, value in enumerate(payload):
            record = _deserialize_record(value, location=f"persisted record {index}")
            key = (record.symbol, record.trading_date)
            if key in records:
                raise MarketPersistenceError(
                    "duplicate natural key in persisted records"
                )
            records[key] = record
        return records

    def _write_records(self, records: Iterable[DailyMarketPrice]) -> None:
        ordered_records = sorted(
            records,
            key=lambda record: (record.symbol, record.trading_date),
        )
        payload = [_serialize_record(record) for record in ordered_records]
        temporary_path = self._path.with_name(f"{self._path.name}.tmp")
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with temporary_path.open("w", encoding="utf-8") as file:
                json.dump(payload, file, indent=2)
                file.write("\n")
            temporary_path.replace(self._path)
        except (OSError, TypeError) as exc:
            try:
                temporary_path.unlink(missing_ok=True)
            except OSError:
                pass
            raise MarketPersistenceError("Could not write market record file") from exc
