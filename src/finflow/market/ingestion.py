"""Application service for retrieving and parsing daily market data."""

from collections.abc import Iterable
from datetime import date

from finflow.market.models import DailyMarketPrice
from finflow.market.parsing import parse_daily_time_series
from finflow.market.persistence import MarketPersistenceResult, MarketRecordStore
from finflow.market.twelve_data import TwelveDataClient


def _normalize_symbol(symbol: str) -> str:
    if not isinstance(symbol, str) or not symbol.strip():
        raise ValueError("Symbol must not be blank")
    return symbol.strip().upper()


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
        payload = self._client.fetch_daily_time_series(
            normalized_symbol,
            start_date=start_date,
            end_date=end_date,
        )
        return parse_daily_time_series(
            payload,
            expected_symbol=normalized_symbol,
        )

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
