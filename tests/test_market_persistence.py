import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from finflow.market.models import DailyMarketPrice
from finflow.market.persistence import (
    JsonMarketRecordStore,
    MarketPersistenceError,
    MarketPersistenceResult,
)


def _record(
    symbol: str = "AAPL",
    trading_date: date = date(2026, 1, 5),
    *,
    open_price: str = "150.1000",
    high_price: str = "155.5000",
    low_price: str = "149.2500",
    close_price: str = "153.3300",
    volume: int | None = 1234567,
) -> DailyMarketPrice:
    return DailyMarketPrice(
        symbol=symbol,
        trading_date=trading_date,
        open=Decimal(open_price),
        high=Decimal(high_price),
        low=Decimal(low_price),
        close=Decimal(close_price),
        volume=volume,
    )


def _serialized_record(
    symbol: str = "AAPL",
    trading_date: str = "2026-01-05",
) -> dict[str, str | int | None]:
    return {
        "symbol": symbol,
        "trading_date": trading_date,
        "open": "150.1000",
        "high": "155.5000",
        "low": "149.2500",
        "close": "153.3300",
        "volume": 1234567,
    }


def test_creates_json_file_with_source_aligned_representation(tmp_path: Path) -> None:
    path = tmp_path / "market.json"
    store = JsonMarketRecordStore(path)

    result = store.upsert(
        [
            _record(),
            _record(
                "MSFT",
                date(2026, 1, 6),
                open_price="420.0100",
                high_price="425.5000",
                low_price="419.2500",
                close_price="423.3300",
                volume=None,
            ),
        ]
    )

    assert result == MarketPersistenceResult(2, 0, 0, 2)
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload == [
        _serialized_record(),
        {
            "symbol": "MSFT",
            "trading_date": "2026-01-06",
            "open": "420.0100",
            "high": "425.5000",
            "low": "419.2500",
            "close": "423.3300",
            "volume": None,
        },
    ]
    assert not (tmp_path / "market.json.tmp").exists()


def test_identical_rerun_is_unchanged_and_not_duplicated(tmp_path: Path) -> None:
    path = tmp_path / "market.json"
    store = JsonMarketRecordStore(path)
    record = _record()
    store.upsert([record])
    original_contents = path.read_text(encoding="utf-8")

    result = store.upsert([record])

    assert result == MarketPersistenceResult(0, 0, 1, 1)
    assert path.read_text(encoding="utf-8") == original_contents
    assert len(json.loads(original_contents)) == 1


def test_changed_natural_key_updates_without_duplicate(tmp_path: Path) -> None:
    path = tmp_path / "market.json"
    store = JsonMarketRecordStore(path)
    store.upsert([_record()])

    result = store.upsert([_record(close_price="154.4400")])

    assert result == MarketPersistenceResult(0, 1, 0, 1)
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert len(payload) == 1
    assert payload[0]["close"] == "154.4400"


def test_writes_records_in_symbol_and_date_order(tmp_path: Path) -> None:
    path = tmp_path / "market.json"
    store = JsonMarketRecordStore(path)

    store.upsert(
        [
            _record("MSFT", date(2026, 1, 5)),
            _record("AAPL", date(2026, 1, 6)),
            _record("AAPL", date(2026, 1, 5)),
        ]
    )

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert [(row["symbol"], row["trading_date"]) for row in payload] == [
        ("AAPL", "2026-01-05"),
        ("AAPL", "2026-01-06"),
        ("MSFT", "2026-01-05"),
    ]


def test_rejects_duplicate_incoming_natural_keys(tmp_path: Path) -> None:
    store = JsonMarketRecordStore(tmp_path / "market.json")

    with pytest.raises(MarketPersistenceError, match="duplicate natural key"):
        store.upsert([_record(), _record(close_price="154.00")])


def test_empty_write_does_not_create_file(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "market.json"

    result = JsonMarketRecordStore(path).upsert([])

    assert result == MarketPersistenceResult(0, 0, 0, 0)
    assert not path.exists()
    assert not path.parent.exists()


def test_creates_missing_parent_directories_when_writing(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "market" / "market.json"

    result = JsonMarketRecordStore(path).upsert([_record()])

    assert result == MarketPersistenceResult(1, 0, 0, 1)
    assert path.exists()


def test_rejects_malformed_json(tmp_path: Path) -> None:
    path = tmp_path / "market.json"
    path.write_text("not-json", encoding="utf-8")

    with pytest.raises(MarketPersistenceError, match="Could not read"):
        JsonMarketRecordStore(path).upsert([])


def test_rejects_wrong_top_level_json_type(tmp_path: Path) -> None:
    path = tmp_path / "market.json"
    path.write_text("{}", encoding="utf-8")

    with pytest.raises(MarketPersistenceError, match="JSON array"):
        JsonMarketRecordStore(path).upsert([])


def test_rejects_malformed_persisted_record(tmp_path: Path) -> None:
    path = tmp_path / "market.json"
    path.write_text(json.dumps([{"symbol": "AAPL"}]), encoding="utf-8")

    with pytest.raises(MarketPersistenceError, match="record shape"):
        JsonMarketRecordStore(path).upsert([])


def test_rejects_duplicate_persisted_natural_keys(tmp_path: Path) -> None:
    path = tmp_path / "market.json"
    record = _serialized_record()
    path.write_text(json.dumps([record, record]), encoding="utf-8")

    with pytest.raises(MarketPersistenceError, match="duplicate natural key"):
        JsonMarketRecordStore(path).upsert([])
