"""Application service for retrieving and parsing daily market data."""

from collections.abc import Iterable
from datetime import date, datetime
from pathlib import Path
from typing import Any

from finstream.bronze.json_storage import raw_json_path, write_raw_json
from finstream.bronze.models import BronzeRunLocation
from finstream.bronze.parquet_storage import parquet_path, write_parquet
from finstream.bronze.paths import DEFAULT_BRONZE_ROOT
from finstream.market.bronze import (
    MARKET_BRONZE_DATASET,
    MARKET_BRONZE_SOURCE,
    MarketBronzeResult,
    daily_market_prices_to_table,
)
from finstream.market.models import DailyMarketPrice
from finstream.market.parsing import parse_daily_time_series
from finstream.market.persistence import MarketPersistenceResult, MarketRecordStore
from finstream.market.twelve_data import TwelveDataClient


def _normalize_symbol(symbol: str) -> str:
    if not isinstance(symbol, str) or not symbol.strip():
        raise ValueError("Symbol must not be blank")
    return symbol.strip().upper()


def _validate_date_range(start_date: date | None, end_date: date | None) -> None:
    if (start_date is None) != (end_date is None):
        raise ValueError("Start date and end date must be supplied together")
    if start_date is not None and end_date is not None and start_date > end_date:
        raise ValueError("Start date must not be after end date")


class MarketIngestionService:
    """Coordinate Twelve Data retrieval and daily market parsing."""

    def __init__(
        self,
        client: TwelveDataClient,
        store: MarketRecordStore | None = None,
    ) -> None:
        self._client = client
        self._store = store

    def ingest_symbol(
        self,
        symbol: str,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[DailyMarketPrice]:
        """Retrieve and parse daily prices for one symbol."""
        normalized_symbol = _normalize_symbol(symbol)
        _, records = self._fetch_and_parse_symbol(
            normalized_symbol,
            start_date=start_date,
            end_date=end_date,
        )
        return records

    def ingest_symbol_to_bronze(
        self,
        symbol: str,
        *,
        run_at: datetime,
        bronze_root: str | Path = DEFAULT_BRONZE_ROOT,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> MarketBronzeResult:
        """Retrieve one provider payload and persist Market Bronze artifacts."""
        normalized_symbol = _normalize_symbol(symbol)
        location = BronzeRunLocation.from_run(
            root=bronze_root,
            source=MARKET_BRONZE_SOURCE,
            dataset=MARKET_BRONZE_DATASET,
            ingested_at=run_at,
        )
        _validate_date_range(start_date, end_date)
        target_raw_json_path = raw_json_path(location)
        target_parquet_path = parquet_path(location)
        if target_raw_json_path.exists() or target_parquet_path.exists():
            raise FileExistsError(
                "Market Bronze artifact already exists for this run"
            )

        payload, records = self._fetch_and_parse_symbol(
            normalized_symbol,
            start_date=start_date,
            end_date=end_date,
        )
        table = daily_market_prices_to_table(records)
        persisted_raw_json_path = write_raw_json(location, payload)
        persisted_parquet_path = write_parquet(location, table)

        return MarketBronzeResult(
            symbol=normalized_symbol,
            location=location,
            raw_json_path=persisted_raw_json_path,
            parquet_path=persisted_parquet_path,
            record_count=len(records),
        )

    def _fetch_and_parse_symbol(
        self,
        normalized_symbol: str,
        *,
        start_date: date | None,
        end_date: date | None,
    ) -> tuple[dict[str, Any], list[DailyMarketPrice]]:
        payload = self._client.fetch_daily_time_series(
            normalized_symbol,
            start_date=start_date,
            end_date=end_date,
        )
        records = parse_daily_time_series(
            payload,
            expected_symbol=normalized_symbol,
        )
        return payload, records


    def ingest_symbols(
        self,
        symbols: Iterable[str],
        *,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> dict[str, list[DailyMarketPrice]]:
        """Retrieve and parse daily prices sequentially for multiple symbols."""
        normalized_symbols: list[str] = []
        seen_symbols: set[str] = set()
        for symbol in symbols:
            normalized_symbol = _normalize_symbol(symbol)
            if normalized_symbol in seen_symbols:
                raise ValueError(f"Duplicate symbol: {normalized_symbol}")
            normalized_symbols.append(normalized_symbol)
            seen_symbols.add(normalized_symbol)

        results: dict[str, list[DailyMarketPrice]] = {}
        for normalized_symbol in normalized_symbols:
            results[normalized_symbol] = self.ingest_symbol(
                normalized_symbol,
                start_date=start_date,
                end_date=end_date,
            )
        return results

    def ingest_and_persist_symbol(
        self,
        symbol: str,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> MarketPersistenceResult:
        """Retrieve, parse, and persist daily prices for one symbol."""
        store = self._require_store()
        records = self.ingest_symbol(
            symbol,
            start_date=start_date,
            end_date=end_date,
        )
        return store.upsert(records)

    def ingest_and_persist_symbols(
        self,
        symbols: Iterable[str],
        *,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> MarketPersistenceResult:
        """Retrieve a complete symbol batch before persisting it once."""
        store = self._require_store()
        records_by_symbol = self.ingest_symbols(
            symbols,
            start_date=start_date,
            end_date=end_date,
        )
        records = [
            record
            for symbol_records in records_by_symbol.values()
            for record in symbol_records
        ]
        return store.upsert(records)

    def _require_store(self) -> MarketRecordStore:
        if self._store is None:
            raise RuntimeError("Market record store is not configured")
        return self._store
